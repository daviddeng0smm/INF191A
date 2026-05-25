package com.pm.backend.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pm.backend.handler.FlightWebSocketHandler;
import com.pm.backend.model.HistoricalFlightObject;
import com.pm.backend.model.TrailMessage;
import com.pm.backend.model.TrailPoint;
import lombok.Getter;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class PlaybackManager {

    private final FlightWebSocketHandler webSocketHandler;
    private final ObjectMapper objectMapper;
    private final Map<String, TreeMap<Long, HistoricalFlightObject>> identTimelines = new ConcurrentHashMap<>();


    private final Set<String> knownIdents = ConcurrentHashMap.newKeySet();

    @Getter
    public long currentPlaybackTime = 0;

    @Getter
    private double timeMultiplier = 1;

    @Getter
    private boolean isPaused = true;

    @Lazy
    public PlaybackManager(FlightWebSocketHandler webSocketHandler, ObjectMapper objectMapper) {
        this.webSocketHandler = webSocketHandler;
        this.objectMapper = objectMapper;
    }

    long earliestTime = Long.MAX_VALUE;
    long latestTime = Long.MIN_VALUE;

    public void addFlightData(HistoricalFlightObject flight) {
        identTimelines
                .computeIfAbsent(flight.ident(), k -> new TreeMap<>())
                .put(flight.clock(), flight);

        // Dynamic Bound Tracking
        this.earliestTime = Math.min(this.earliestTime, flight.clock());
        this.latestTime = Math.max(this.latestTime, flight.clock());

        if (currentPlaybackTime == 0 || flight.clock() < currentPlaybackTime) {
            currentPlaybackTime = flight.clock();
        }
    }

    public void broadcastTrails() throws JsonProcessingException {
        for (Map.Entry<String, TreeMap<Long, HistoricalFlightObject>> e : identTimelines.entrySet()) {
            List<TrailPoint> points = e.getValue().values().stream()
                    .map(f -> new TrailPoint(f.clock(), f.lat(), f.lon(), f.alt(), f.heading()))
                    .toList();
            TrailMessage msg = new TrailMessage("TRAIL", e.getKey(), points);
            webSocketHandler.broadcastFlight(objectMapper.writeValueAsString(msg));
        }
    }

    public void clearData(){
        this.identTimelines.clear();
    }




    @Scheduled(fixedRate = 1000)
    public void tick() {
        if (isPaused || identTimelines.isEmpty()) return;
        // streams each plane's position at the current playback time, interpolating if necessary
        for (TreeMap<Long, HistoricalFlightObject> timeline : identTimelines.values()) {
            // Binary search for the bounding points
            Map.Entry<Long, HistoricalFlightObject> p1Entry = timeline.floorEntry(currentPlaybackTime);
            if (p1Entry == null) continue;

            Map.Entry<Long, HistoricalFlightObject> p2Entry = timeline.higherEntry(currentPlaybackTime);
            HistoricalFlightObject p1 = p1Entry.getValue();

            if (p1Entry.getKey() == currentPlaybackTime) {
                webSocketHandler.broadcastFlight(p1);
                continue;
            }


            if (p2Entry != null) {
                HistoricalFlightObject p2 = p2Entry.getValue();
                double timeGap = (double)(p2.clock() - p1.clock());
                double elapsed = (double)(currentPlaybackTime - p1.clock());
                double ratio = timeGap > 0 ? elapsed / timeGap : 0.0;

                HistoricalFlightObject ghost = interpolate(p1, p2, ratio);
                webSocketHandler.broadcastFlight(ghost);
            } else {
                webSocketHandler.broadcastFlight(p1);
            }
        }
        currentPlaybackTime += timeMultiplier;    }


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
                p1.gs(),                 // 10
                p1.orig(),               // 11
                p1.dest(),               // 12
                p1.aircrafttype(),       // 13
                p1.status(),             // 14
                p1.squawk(),             // 15
                p1.actual_runway_off(),  // 16
                p1.actual_runway_on()    // 17
        );
    }

    // --- Control methods for playback ---
    //check if time is valid
    boolean isValidTime(long targetEpochTime) {
        if (identTimelines.isEmpty()) return false;
        return targetEpochTime >= earliestTime && targetEpochTime <= latestTime;
    }

    // jump to a time
    public void jumpToTime(long targetEpoch) {
        if (!isValidTime(targetEpoch)) {
            String errorMsg = "{\"type\": \"ERROR\", \"message\": \"Time " + targetEpoch + " is out of bounds!\"}";
            webSocketHandler.broadcastFlight(errorMsg);
            return;
        }

        // save the current pause state
        boolean wasPaused = this.isPaused;
        // set the new time
        this.currentPlaybackTime = targetEpoch;
        //  unpause so tick can bypass its own check
        this.isPaused = false;
        tick();
        // restore the previous pause state
        this.isPaused = wasPaused;
    }


    // pause
    public void setPaused() {
        this.isPaused = true;
    }
    // resume
    public void setResume() {
        this.isPaused = false;
    }

    //playback speed
    public void setSpeed(double speed) {
        this.timeMultiplier = speed;
    }
}