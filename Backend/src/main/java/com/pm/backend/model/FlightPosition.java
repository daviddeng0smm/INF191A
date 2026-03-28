package com.pm.backend.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public record FlightPosition(
        String ident,
        double lat,
        double lon,
        double alt,
        double heading,
        long clock
) {}