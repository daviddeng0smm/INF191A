package com.pm.backend;

import com.pm.backend.services.FlightStreamer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BackendApplication implements CommandLineRunner {

    // 1. @Autowired tells Spring: "Please grab the FlightStreamer we built and put it here."
    @Autowired
    private FlightStreamer flightStreamer;

    public static void main(String[] args) {
        SpringApplication.run(BackendApplication.class, args);
    }

    // 2. Because we added "implements CommandLineRunner" at the top,
    // Spring Boot guarantees it will run this exact method right after the server boots up.
    @Override
    public void run(String... args) {
        System.out.println("Spring Boot is live! Starting the Firehose...");

        // 3. We define a standard background task
        Runnable backgroundTask = new Runnable() {
            @Override
            public void run() {
                // This is where we actually turn the streamer on
                flightStreamer.startLiveStreaming("KLAX");
            }
        };

        // 4. We hand that task to a new Thread and tell it to start working
        Thread workerThread = new Thread(backgroundTask);
        workerThread.start();
    }
}