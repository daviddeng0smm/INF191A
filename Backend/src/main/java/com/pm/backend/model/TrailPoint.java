package com.pm.backend.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TrailPoint(
        long clock,
        double lat,
        double lon,
        double alt,
        double heading
) {}
