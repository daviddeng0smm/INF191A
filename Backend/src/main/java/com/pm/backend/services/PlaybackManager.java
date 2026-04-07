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
        flightMap.computeIfAbsent(flight.clock(), k -> new CopyOnWriteArrayList<>()).add(flight);        // Automatically set the start time to the first piece of data received
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

        // 1. Find the real data point immediately BEFORE current time
        Long t1 = flightMap.floorKey(currentPlaybackTime);
        // 2. Find the real data point immediately AFTER current time
        Long t2 = flightMap.higherKey(t1);

        if (t1 != null && t2 != null) {
            // We are "between" two real points. Let's calculate the "Ghost" now!
            HistoricalFlightObject p1 = flightMap.get(t1).get(0);
            HistoricalFlightObject p2 = flightMap.get(t2).get(0);

            double ratio = (double)(currentPlaybackTime - t1) / (t2 - t1);

            // Calculate the temporary position
            double lerpLat = p1.lat() + (p2.lat() - p1.lat()) * ratio;
            double lerpLon = p1.lon() + (p2.lon() - p1.lon()) * ratio;
            double lerpAlt = p1.alt() + (p2.alt() - p1.alt()) * ratio;

            // Create a temporary "Ghost" just for this message
            HistoricalFlightObject currentFrame = new HistoricalFlightObject(
                    p1.id(),
                    p1.ident(),
                    "interpolated", // type
                    currentPlaybackTime,
                    lerpLat,
                    lerpLon,
                    lerpAlt,
                    p1.groundspeed(),
                    p1.heading(),
                    p1.orig(),
                    p1.dest(),
                    p1.aircrafttype(),
                    p1.status(),
                    p1.actual_runway_off(),
                    p1.actual_runway_on()
            );

            webSocketHandler.broadcastFlight(currentFrame); // Send it and let it be deleted from memory
        }
        else if (t1 != null) {
            // We are at the very end of the data, just send the last point
            webSocketHandler.broadcastFlight(flightMap.get(t1).get(0));
        }

        currentPlaybackTime++;
    }



    // --- Control Methods ---
    public void jumpToTime(long targetEpoch) {
        this.currentPlaybackTime = targetEpoch;
        System.out.println("Scrubbed to: " + targetEpoch);

        // Optional: Trigger one immediate broadcast so Unity updates instantly
        // instead of waiting for the next 1-second tick.
        System.out.println("Seeked to: " + currentPlaybackTime);
        tick();
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