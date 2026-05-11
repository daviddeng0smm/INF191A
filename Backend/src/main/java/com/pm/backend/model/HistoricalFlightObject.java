package com.pm.backend.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public record HistoricalFlightObject(
        // Mandatory Identification
        String id,      // Unique FlightAware ID
        String ident,   // Callsign (e.g., SWA123)
        String type,    // Message type (position or flifo)
        long clock,

        // Movement Data (Position)
        double lat,
        double lon,
        double alt,
        double groundspeed,
        double heading,
        double gs,

        // Flight Context (Flifo)
        String orig,    // Origin Airport
        String dest,    // Destination Airport
        String aircrafttype, // e.g., B738
        String status,  // S, F, A, Z, or X
        String squawk,
        // Runway Info (Great for VR replays!)
        String actual_runway_off,
        String actual_runway_on
) {}
