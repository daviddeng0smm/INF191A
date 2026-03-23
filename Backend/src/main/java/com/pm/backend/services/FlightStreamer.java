package com.pm.backend.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pm.backend.config.FirehoseConnector;
import com.pm.backend.handler.FlightWebSocketHandler;
import com.pm.backend.model.FlightPosition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.net.ssl.SSLSocket;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.TreeMap;


@Service
public class FlightStreamer {

    @Value("${flightaware.username}")
    private String username;

    @Value("${flightaware.apikey}")
    private String apikey;

    private final ObjectMapper objectMapper;
    private final FirehoseConnector firehoseConnector;
    private final FlightWebSocketHandler webSocketHandler;
    public static TreeMap<Integer, FlightPosition[] >a;

    // Spring Boot automatically injects both tools now
    public FlightStreamer(FirehoseConnector firehoseConnector, FlightWebSocketHandler webSocketHandler) {
        this.firehoseConnector = firehoseConnector;
        this.webSocketHandler = webSocketHandler;
        this.objectMapper = new ObjectMapper();
    }
    @Async
    public void startLiveStreaming(String airportCode) {
        try {
            SSLSocket socket = firehoseConnector.createSecureConnection();
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            System.out.println("Authenticating and requesting live data " + airportCode + "...");
            String initCommand = String.format("live username %s password %s events \"position\" airport_filter \"%s\"",
                    username, apikey, airportCode);
            out.println(initCommand);

            String rawJsonLine;
            // Define the LAX Bounding Box at the class level or inside the loop
            double minLat = 32.5;
            double maxLat = 35.0;
            double minLon = -120.5;
            double maxLon = -114.5;

            while ((rawJsonLine = in.readLine()) != null) {
                try {
                    FlightPosition flight = objectMapper.readValue(rawJsonLine, FlightPosition.class);
                    System.out.println("Waiting for a nearby plane");
                    // 1. ADD THE BOUNDING BOX FILTER HERE
                    boolean isAtLAX = flight.lat() >= minLat && flight.lat() <= maxLat &&
                            flight.lon() >= minLon && flight.lon() <= maxLon;

                    // 2. Only broadcast if it is a valid coordinate AND inside the box
                    if (flight.lat() != 0.0 && flight.lon() != 0.0 && isAtLAX) {
                        System.out.println("Tracking LAX -> " + flight.ident());

                        String cleanJson = objectMapper.writeValueAsString(flight);
                        webSocketHandler.broadcastFlight(cleanJson);
                    }
                } catch (Exception e) {
                    // Ignore malformed JSON lines
                }
            }
        } catch (Exception e) {
            System.err.println("Connection dropped or failed: " + e.getMessage());
        }
    }

    @Async
    public void startHistoricalStream(String [] planeIdentifierList, Long startTime, Long endTime, String airportCode) throws IOException {
        SSLSocket socket = firehoseConnector.createSecureConnection();
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        String planeIdentifiers = String.join(" ", planeIdentifierList);

        try{
            System.out.println("Authenticating and requesting historical data" +  "planeIdentifiers: " + planeIdentifiers
                    + "startTime: " + startTime + "endTime: " + endTime);
            String initCommand = String.format("range %s startTime %s endTime username %s password %s events \"flifo\" airport_filter \"%s\" ",
                    startTime, endTime, username, apikey,  );
            out.println(initCommand);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }


}