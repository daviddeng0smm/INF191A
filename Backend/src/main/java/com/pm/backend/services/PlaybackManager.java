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

        // Find the data for the current second or the closest one before it
        Long actualKey = flightMap.floorKey(currentPlaybackTime);

        if (actualKey != null) {
            List<HistoricalFlightObject> flights = flightMap.get(actualKey);
            try {
                String json = objectMapper.writeValueAsString(flights);
                webSocketHandler.broadcastFlight(json);
            } catch (Exception e) {
                System.err.println("Playback error: " + e.getMessage());
            }
        }

        currentPlaybackTime++; // Advance the "Playhead" by 1 second
    }

    // --- Control Methods ---

    public void seek(int secondsOffset) {
        this.currentPlaybackTime += secondsOffset; // Handles -10 or +10
        System.out.println("Seeked to: " + currentPlaybackTime);
    }

    public void setPaused(boolean paused) {
        this.isPaused = paused;
    }
}