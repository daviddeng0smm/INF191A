package com.pm.backend.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pm.backend.config.FirehoseConnector;
import com.pm.backend.handler.FlightWebSocketHandler;
import com.pm.backend.model.FlightPosition;
import com.pm.backend.model.HistoricalFlightObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.net.ssl.SSLSocket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@Service
public class HistoricalStreamer {
    @Value("${flightaware.username}")
    private String username;

    @Value("${flightaware.apikey}")
    private String apikey;

    private final ObjectMapper objectMapper;
    private final FirehoseConnector firehoseConnector;
    private final FlightWebSocketHandler flightWebSocketHandler;

    // Standard constructor for Spring injection
    public HistoricalStreamer(FlightWebSocketHandler flightWebSocketHandler,
                              FirehoseConnector firehoseConnector,
                              ObjectMapper objectMapper) {
        this.flightWebSocketHandler = flightWebSocketHandler;
        this.firehoseConnector = firehoseConnector;
        this.objectMapper = objectMapper;
    }

    String content = "Hello, world! This is a simple text file.";
    String filepath = "C:\\Users\\David\\Desktop\\INF191A\\Backend\\src\\output.txt";

    @Async
    public void StartHistoricalStreamer(String[] airplaneIdentifiers, long epochStartTime, long epochEndTime) {
        try {
            // 1. Establish the secure tunnel
            SSLSocket socket = firehoseConnector.createSecureConnection();
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // 2. Prepare the filter list
            String filteredList = String.join(" ", airplaneIdentifiers);

            System.out.println("Authenticating and requesting historical data for: " + filteredList);

            // 3. FIX: Added 'username' to the format parameters so 'apikey' doesn't take its place
            String initCommand = String.format(
                    "range %d %d username %s password %s events \"position flifo\" idents \"%s\"",
                    epochStartTime, epochEndTime, username, apikey, filteredList
            );

            out.println(initCommand); // Send the "Hello" command

            String rawJsonLine;
            // 4. The main data loop
            while ((rawJsonLine = in.readLine()) != null) {
                try {
                    Object json = objectMapper.readValue(rawJsonLine, Object.class);
                    String prettyJson = objectMapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(json);

                    // FIX: Wrap 'filepath' in Paths.get()
                    Files.writeString(Paths.get(filepath), prettyJson + System.lineSeparator(),
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND);

                    System.out.println("Saved message to file: " + filepath);

                } catch (Exception e) {
                    System.out.println("Skipping non-JSON message: " + rawJsonLine);
                }
            }

            // Clean up resources if the stream ends
            socket.close();
            System.out.println("Historical stream ended gracefully.");

        } catch (Exception e) {
            System.err.println("Connection dropped or failed: " + e.getMessage());
        }
    }
}