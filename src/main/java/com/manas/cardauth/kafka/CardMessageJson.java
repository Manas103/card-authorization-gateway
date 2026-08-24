package com.manas.cardauth.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.manas.cardauth.model.CardMessage;

import java.io.UncheckedIOException;

/**
 * The single shared JSON mapping for {@link CardMessage}, used both by the
 * Spring-managed Kafka (de)serializers in {@link KafkaConfig} and,
 * directly, by the benchmark runner's raw {@code KafkaProducer}/
 * {@code KafkaConsumer} (which has no Spring context to inject a bean
 * from). One mapping, one place {@code Instant}/{@code BigDecimal}
 * handling can go wrong.
 */
public final class CardMessageJson {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private CardMessageJson() {
    }

    public static String toJson(CardMessage message) {
        try {
            return MAPPER.writeValueAsString(message);
        } catch (Exception e) {
            throw new UncheckedIOException(new java.io.IOException(e));
        }
    }

    public static CardMessage fromJson(String json) {
        try {
            return MAPPER.readValue(json, CardMessage.class);
        } catch (Exception e) {
            throw new UncheckedIOException(new java.io.IOException(e));
        }
    }
}
