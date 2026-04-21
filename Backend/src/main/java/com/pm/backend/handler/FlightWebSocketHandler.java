package com.pm.backend.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pm.backend.model.HistoricalFlightObject;
import com.pm.backend.services.PlaybackManager;
import com.pm.backend.services.HistoricalFirehoseIngestor;
import com.pm.backend.services.LiveStreamer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class FlightWebSocketHandler extends TextWebSocketHandler {
    private final CopyOnWriteArrayList<WebSocketSession> sessions = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper;


    @Autowired
    @Lazy
    private HistoricalFirehoseIngestor historicalFirehoseIngestor;

    @Autowired
    @Lazy
    private LiveStreamer liveStreamer;

    @Autowired
    @Lazy
    private PlaybackManager playbackManager;

    public FlightWebSocketHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.add(session);
        System.out.println("Unity Game Engine Connected! Session ID: " + session.getId());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session);
        System.out.println("Unity Game Engine Disconnected!");
    }

    // action:START_LIVE, ,
    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        JsonNode json = objectMapper.readTree(payload);
        String action = json.get("action").asText();

        switch(action){
            case "START_LIVE":
                String airportName = json.get("Airport").asText();
                liveStreamer.startLiveStreaming(airportName);
                break;
            case "START_HISTORICAL":
                liveStreamer.stop();
                String [] identifiers = json.get("identifiers").asText().split(",");
                long start = json.get("startTime").asLong();
                long end = json.get("endTime").asLong();
                playbackManager.clearData();
                historicalFirehoseIngestor.StartHistoricalStreamer(identifiers, start, end)
                        .thenAccept(success -> playbackManager.setPaused(false));
                break;
            case "SEEK":
                long targetTime = json.get("targetTime").asLong();
                playbackManager.jumpToTime(targetTime);
                break;
        }
    }

    public void broadcastFlight(String jsonFlightData) {
        for (WebSocketSession session : sessions) {
            logToFile(jsonFlightData);
            if (session.isOpen()) {
                try {
                    session.sendMessage(new TextMessage(jsonFlightData));
                } catch (IOException e) {
                    System.err.println("Failed to send flight to Unity: " + e.getMessage());
                }
            }
        }
    }
    public void broadcastFlight(HistoricalFlightObject flightData) {
        try {
            // Convert the object to string and call Version A
            String json = objectMapper.writeValueAsString(flightData);
            broadcastFlight(json);
        } catch (Exception e) {
            System.err.println("Error serializing historical flight: " + e.getMessage());
        }
    }

    private void logToFile(String data) {
        try {
            String logPath = "C:\\Users\\David\\Desktop\\INF191A\\Backend\\src\\broadcast_log.txt";
            Files.writeString(Paths.get(logPath), data + System.lineSeparator(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            // We catch it here so a file error doesn't crash the actual live stream
            System.err.println("Logging Error: " + e.getMessage());
        }
    }
}