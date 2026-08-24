package com.manas.cardauth.model;

/**
 * The three message kinds this gateway consumes off the Kafka stream
 * (claim 1). {@code AUTHORIZATION} places a hold; exactly one terminal
 * event, either {@code REVERSAL} (the hold is voided, no money moves) or
 * {@code CLEARING} (the hold is released and the funds are posted), closes
 * a card transaction out.
 */
public enum MessageType {
    AUTHORIZATION,
    REVERSAL,
    CLEARING
}
