package com.pm.backend.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pm.backend.services.LiveStreamer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class FlightWebSocketHandler extends TextWebSocketHandler {
    private final CopyOnWriteArrayList<WebSocketSession> sessions = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper;

    @Autowired
    @Lazy
    private LiveStreamer liveStreamer;

    public FlightWebSocketHandler() {
        this.objectMapper = new ObjectMapper();
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
                String ident = json.get("ident").asText();
                long start = json.get("startTime").asLong();
                long end = json.get("endTime").asLong();

                break;
        }


    }

    public void broadcastFlight(String jsonFlightData) {
        for (WebSocketSession session : sessions) {
            if (session.isOpen()) {
                try {
                    session.sendMessage(new TextMessage(jsonFlightData));
                } catch (IOException e) {
                    System.err.println("Failed to send flight to Unity: " + e.getMessage());
                }
            }
        }
    }
}