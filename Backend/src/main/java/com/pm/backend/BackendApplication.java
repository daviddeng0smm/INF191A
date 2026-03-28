package com.pm.backend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pm.backend.services.HistoricalStreamer;
import com.pm.backend.services.LiveStreamer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync // This turns on the background worker feature
public class BackendApplication implements CommandLineRunner {
    @Lazy
    @Autowired
    private LiveStreamer liveStreamer;

    @Lazy
    @Autowired
    private HistoricalStreamer historicalStreamer;


    public static void main(String[] args) {
        SpringApplication.run(BackendApplication.class, args);
    }
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Override
    public void run(String... args) {
        System.out.println("Spring Boot is live! Starting the Firehose via Async...");

        // This call now returns INSTANTLY because of @Async.
        // The main thread doesn't wait for the loop to finish!
//        historicalStreamer.StartHistoricalStreamer(new String[]{"DAL371", "AAL125"}, 1774270800, 1774272600);
        liveStreamer.startLiveStreaming("KLAX");
        System.out.println("Main thread is free to handle WebSockets!");
    }
}