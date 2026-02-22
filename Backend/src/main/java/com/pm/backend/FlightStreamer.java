package com.pm.backend;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.net.ssl.SSLSocket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;

@Service
public class FlightStreamer {

    @Value("${flightaware.username}")
    private String username;

    @Value("${flightaware.apikey}")
    private String apikey;

    private final ObjectMapper objectMapper;
    private final FirehoseConnector firehoseConnector;

    // Spring Boot automatically injects the FirehoseConnector when it builds this class
    public FlightStreamer(FirehoseConnector firehoseConnector) {
        this.firehoseConnector = firehoseConnector;
        this.objectMapper = new ObjectMapper();
    }

    public void startStreaming(String airportCode) {
        try {
            // 1. Get the raw, open tunnel from your Connection Manager
            SSLSocket socket = firehoseConnector.createSecureConnection();

            // 2. Set up the tools to push and pull text through the tunnel
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // 3. Send the Handshake Command
            System.out.println("Authenticating and requesting " + airportCode + "...");
            String initCommand = String.format("live username %s password %s events \"position\" airport_filter \"%s\"",
                    username, apikey, airportCode);
            out.println(initCommand);

            // 4. Catch the infinite stream
            String rawJsonLine;
            while ((rawJsonLine = in.readLine()) != null) {
                try {
                    // 5. Translate JSON string to clean Java Record
                    FlightPosition flight = objectMapper.readValue(rawJsonLine, FlightPosition.class);

                    if (flight.lat() != 0.0 && flight.lon() != 0.0) {
                        System.out.println("Tracking -> " + flight.ident() + " at Lat: " + flight.lat() + ", Lon: " + flight.lon());
                    }
                } catch (Exception e) {
                    // Ignore malformed JSON lines
                }
            }
        } catch (Exception e) {
            System.err.println("Connection dropped or failed: " + e.getMessage());
        }
    }
}