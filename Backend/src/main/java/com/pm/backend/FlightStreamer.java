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
    private final FlightWebSocketHandler webSocketHandler;

    // Spring Boot automatically injects both tools now
    public FlightStreamer(FirehoseConnector firehoseConnector, FlightWebSocketHandler webSocketHandler) {
        this.firehoseConnector = firehoseConnector;
        this.webSocketHandler = webSocketHandler;
        this.objectMapper = new ObjectMapper();
    }

    public void startStreaming(String airportCode) {
        try {
            SSLSocket socket = firehoseConnector.createSecureConnection();
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            System.out.println("Authenticating and requesting " + airportCode + "...");
            String initCommand = String.format("live username %s password %s events \"position\" airport_filter \"%s\"",
                    username, apikey, airportCode);
            out.println(initCommand);

            String rawJsonLine;
            while ((rawJsonLine = in.readLine()) != null) {
                try {
                    FlightPosition flight = objectMapper.readValue(rawJsonLine, FlightPosition.class);

                    if (flight.lat() != 0.0 && flight.lon() != 0.0) {
                        System.out.println("Tracking -> " + flight.ident() + " at Lat: " + flight.lat() + ", Lon: " + flight.lon());

                        // Shove the exact same data to any connected Unity games!
                        webSocketHandler.broadcastFlight(rawJsonLine);
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