package com.pm.backend.config;

import org.springframework.stereotype.Component;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;

// @Component tells Spring Boot: "Hey, create one instance of this tool
// and keep it ready in case any other part of the app needs it."
@Component
public class FirehoseConnector {


    public SSLSocket createSecureConnection() throws IOException {
        System.out.println("Building secure TCP tunnel to FlightAware...");

        String serverName = "firehose.flightaware.com";
        int port = 1501;

        // 1. Get the Java SSL Factory (the tool that builds encrypted sockets)
        SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();

        // 2. Build the socket and dial the server
        SSLSocket socket = (SSLSocket) factory.createSocket(serverName, port);

        System.out.println("Tunnel connected successfully!");

        // 3. Return the raw, empty, secure tunnel
        return socket;
    }
}