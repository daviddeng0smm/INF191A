package com.pm.backend.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TrailMessage(
        String type,
        String ident,
        List<TrailPoint> points
) {}
