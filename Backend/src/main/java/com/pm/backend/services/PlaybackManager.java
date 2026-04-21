package com.pm.backend.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pm.backend.handler.FlightWebSocketHandler;
import com.pm.backend.model.HistoricalFlightObject;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.util.NavigableMap;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class PlaybackManager {

    private final TreeMap<Long, List<HistoricalFlightObject>> flightMap = new TreeMap<>();
    private final FlightWebSocketHandler webSocketHandler;
    private final ObjectMapper objectMapper;
    private final Map<String, TreeMap<Long, HistoricalFlightObject>> identTimelines = new ConcurrentHashMap<>();


    private final Set<String> knownIdents = ConcurrentHashMap.newKeySet();

    private long currentPlaybackTime = 0;
    private boolean isPaused = true;

    @Lazy
    public PlaybackManager(FlightWebSocketHandler webSocketHandler, ObjectMapper objectMapper) {
        this.webSocketHandler = webSocketHandler;
        this.objectMapper = objectMapper;
    }

    public void addFlightData(HistoricalFlightObject flight) {
        // Keep existing flightMap for anything else that uses it
        flightMap.computeIfAbsent(flight.clock(), k -> new CopyOnWriteArrayList<>()).add(flight);

        // Also index by ident for O(log N) per-plane lookups
        identTimelines
                .computeIfAbsent(flight.ident(), k -> new TreeMap<>())
                .put(flight.clock(), flight);

        if (currentPlaybackTime == 0 || flight.clock() < currentPlaybackTime) {
            currentPlaybackTime = flight.clock();
        }
    }

    public void clearData(){
        this.flightMap.clear();
    }

    @Scheduled(fixedRate = 1000)
    public void tick() {
        if (isPaused || identTimelines.isEmpty()) return;

        for (TreeMap<Long, HistoricalFlightObject> timeline : identTimelines.values()) {
            // Binary search for the bounding points
            Map.Entry<Long, HistoricalFlightObject> p1Entry = timeline.floorEntry(currentPlaybackTime);
            if (p1Entry == null) continue;

            Map.Entry<Long, HistoricalFlightObject> p2Entry = timeline.higherEntry(currentPlaybackTime);
            HistoricalFlightObject p1 = p1Entry.getValue();

            if (p2Entry != null) {
                HistoricalFlightObject p2 = p2Entry.getValue();
                double timeGap = (double)(p2.clock() - p1.clock());
                double elapsed = (double)(currentPlaybackTime - p1.clock());
                double ratio = timeGap > 0 ? elapsed / timeGap : 0.0;

                // DON'T serialize to string here. Send the object directly.
                webSocketHandler.broadcastFlight(interpolate(p1, p2, ratio));
            } else {
                webSocketHandler.broadcastFlight(p1);
            }
        }

        currentPlaybackTime++;
    }

    /**
     * Logic-specific function to determine if a plane should be interpolated
     * or broadcasted as-is.
     */
    private void processAndBroadcastFlight(HistoricalFlightObject p1) {
        Long t2 = findNextTimestampForIdent(p1.ident(), currentPlaybackTime);

        if (t2 != null) {
            List<HistoricalFlightObject> nextList = flightMap.get(t2);
            HistoricalFlightObject p2 = findMatchingPlane(nextList, p1.ident());

            if (p2 != null) {
                double timeGap = (double) (t2 - p1.clock()); // total gap between real points
                double elapsed = (double) (currentPlaybackTime - p1.clock()); // how far we are into that gap
                double ratio = elapsed / timeGap; // 0.0 at p1, 1.0 at p2

                HistoricalFlightObject interpolated = interpolate(p1, p2, ratio);
                webSocketHandler.broadcastFlight(interpolated);
                return;
            }
        }

        webSocketHandler.broadcastFlight(p1);
    }

    private void broadcastSerialized(HistoricalFlightObject flight) {
        try {
            String json = objectMapper.writeValueAsString(flight);
            webSocketHandler.broadcastFlight(json);
        } catch (Exception e) {
            System.err.println("Serialization error: " + e.getMessage());
        }
    }

    private Long findNextTimestampForIdent(String ident, long currentTime) {
        SortedMap<Long, List<HistoricalFlightObject>> futureMap = flightMap.tailMap(currentTime + 1);

        for (Map.Entry<Long, List<HistoricalFlightObject>> entry : futureMap.entrySet()) {
            for (HistoricalFlightObject flight : entry.getValue()) {
                if (flight.ident().equals(ident)) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }

    private Set<String> getAllIdents() {
        Set<String> idents = new HashSet<>();
        for (List<HistoricalFlightObject> flights : flightMap.values()) {
            for (HistoricalFlightObject f : flights) {
                idents.add(f.ident());
            }
        }
        return idents;
    }

    private HistoricalFlightObject findMostRecentForIdent(String ident, long currentTime) {
        NavigableMap<Long, List<HistoricalFlightObject>> pastMap =
                flightMap.headMap(currentTime + 1, true); // inclusive of currentTime

        for (Long key : pastMap.descendingKeySet()) {
            List<HistoricalFlightObject> flights = pastMap.get(key);
            for (HistoricalFlightObject f : flights) {
                if (f.ident().equals(ident)) return f;
            }
        }
        return null;
    }

    private HistoricalFlightObject interpolate(HistoricalFlightObject p1, HistoricalFlightObject p2, double ratio) {
        double newLat = p1.lat() + (p2.lat() - p1.lat()) * ratio;
        double newLon = p1.lon() + (p2.lon() - p1.lon()) * ratio;
        double newAlt = p1.alt() + (p2.alt() - p1.alt()) * ratio;

        // Use currentPlaybackTime for the clock to keep Unity in sync
        return new HistoricalFlightObject(
                p1.id(),                 // 1
                p1.ident(),              // 2
                "interpolated",          // 3
                currentPlaybackTime,     // 4
                newLat,                  // 5
                newLon,                  // 6
                newAlt,                  // 7
                p1.groundspeed(),        // 8
                p1.heading(),            // 9
                p1.orig(),               // 10
                p1.dest(),               // 11
                p1.aircrafttype(),       // 12
                p1.status(),             // 13
                p1.actual_runway_off(),  // 14
                p1.actual_runway_on()    // 15
        );
    }
    // Helper to find the same plane in the next data batch
    private HistoricalFlightObject findMatchingPlane(List<HistoricalFlightObject> list, String ident) {
        if (list == null) return null;
        for (HistoricalFlightObject flight : list) {
            if (flight.ident().equals(ident)) {
                return flight;
            }
        }
        return null;
    }


    // --- Control Methods ---
    public void jumpToTime(long targetEpoch) {
        this.currentPlaybackTime = targetEpoch;
        System.out.println("Scrubbed to: " + targetEpoch);
        // call tick here to immediately update the view after scrubbing
        tick();

        System.out.println("Seeked to: " + currentPlaybackTime);
    }
    public void seek(int secondsOffset) {
        this.currentPlaybackTime += secondsOffset; // Handles -10 or +10
        System.out.println("Seeked to: " + currentPlaybackTime);
    }

    public void setPaused(boolean paused) {
        this.isPaused = paused;
    }

    public TreeMap<Long, List<HistoricalFlightObject>> getFlightMap() {
        return this.flightMap;
    }
}