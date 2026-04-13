package com.pm.backend.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pm.backend.handler.FlightWebSocketHandler;
import com.pm.backend.model.HistoricalFlightObject;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class PlaybackManager {

    private final TreeMap<Long, List<HistoricalFlightObject>> flightMap = new TreeMap<>();
    private final FlightWebSocketHandler webSocketHandler;
    private final ObjectMapper objectMapper;

    private long currentPlaybackTime = 0;
    private boolean isPaused = true;

    public PlaybackManager(FlightWebSocketHandler webSocketHandler, ObjectMapper objectMapper) {
        this.webSocketHandler = webSocketHandler;
        this.objectMapper = objectMapper;
    }

    public void addFlightData(HistoricalFlightObject flight) {
        flightMap.computeIfAbsent(flight.clock(), k -> new CopyOnWriteArrayList<>()).add(flight);
        System.out.println("Added flight data: " + flightMap.get(flight.clock()) + "to key " +  flight.clock() );
        // Automatically set the start time to the first piece of data received
        if (currentPlaybackTime == 0 || flight.clock() < currentPlaybackTime) {
            currentPlaybackTime = flight.clock();
        }
    }

    public void clearData(){
        this.flightMap.clear();
    }

    // This method is called by a timer or a loop to advance the replay
    @Scheduled(fixedRate = 1000)
    public void tick() {
        if (isPaused || flightMap.isEmpty()) return;

        // 1. Get the closest recorded time at or before current time
        Long t1 = flightMap.floorKey(currentPlaybackTime);

        // SAFETY: If current time is exactly a recorded timestamp, broadcast the real data
        if (t1 != null && t1 == currentPlaybackTime) {
            for (HistoricalFlightObject realData : flightMap.get(t1)) {
                webSocketHandler.broadcastFlight(realData); // Keeps type: "position"
            }
            currentPlaybackTime++;
            return;
        }

        // 2. Otherwise, perform interpolation calculation
        Long t2 = flightMap.higherKey(t1);
        if (t1 != null && t2 != null) {
            double ratio = (double)(currentPlaybackTime - t1) / (t2 - t1);

            for (HistoricalFlightObject p1 : flightMap.get(t1)) {
                HistoricalFlightObject p2 = findMatchingPlane(p1.ident(), flightMap.get(t2));

                if (p2 != null) {
                    // Calculate interpolated values
                    double lerpLat = p1.lat() + (p2.lat() - p1.lat()) * ratio;
                    double lerpLon = p1.lon() + (p2.lon() - p1.lon()) * ratio;
                    double lerpAlt = p1.alt() + (p2.alt() - p1.alt()) * ratio;

                    HistoricalFlightObject currentFrame = new HistoricalFlightObject(
                            p1.id(), p1.ident(), "interpolated", currentPlaybackTime,
                            lerpLat, lerpLon, lerpAlt,
                            p1.groundspeed(), p1.heading(), p1.orig(), p1.dest(),
                            p1.aircrafttype(), p1.status(), p1.actual_runway_off(), p1.actual_runway_on()
                    );

                    webSocketHandler.broadcastFlight(currentFrame);
                }
            }
        }
        currentPlaybackTime++;
    }

    // Helper to find the same plane in the next data batch
    private HistoricalFlightObject findMatchingPlane(String ident, List<HistoricalFlightObject> nextPlanes) {
        return nextPlanes.stream()
                .filter(p -> p.ident().equals(ident))
                .findFirst()
                .orElse(null);
    }


    // --- Control Methods ---
    public void jumpToTime(long targetEpoch) {
        this.currentPlaybackTime = targetEpoch;
        System.out.println("Scrubbed to: " + targetEpoch);

        tick();
        // Optional: Trigger one immediate broadcast so Unity updates instantly
        // instead of waiting for the next 1-second tick.
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