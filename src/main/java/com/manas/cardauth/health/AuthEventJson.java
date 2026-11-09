package com.manas.cardauth.health;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.UncheckedIOException;

/**
 * The single shared JSON mapping for {@link AuthEvent}, the same
 * one-mapping-one-place convention {@code CardMessageJson} uses for the
 * ledger engine's messages.
 */
public final class AuthEventJson {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private AuthEventJson() {
    }

    public static String toJson(AuthEvent event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (Exception e) {
            throw new UncheckedIOException(new java.io.IOException(e));
        }
    }

    public static AuthEvent fromJson(String json) {
        try {
            return MAPPER.readValue(json, AuthEvent.class);
        } catch (Exception e) {
            throw new UncheckedIOException(new java.io.IOException(e));
        }
    }
}
