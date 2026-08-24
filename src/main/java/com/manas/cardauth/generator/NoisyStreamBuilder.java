package com.manas.cardauth.generator;

import com.manas.cardauth.model.CardMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Turns a canonical, in-order message stream into a noisy one: a fixed
 * fraction of messages are redelivered as exact duplicates (same
 * messageId and traceNumber, simulating broker/producer at-least-once
 * redelivery, not a distinct new message), and the whole stream is
 * shuffled out of transaction order. Used for the idempotent-replay
 * measurement (claim 4): the engine's final ledger state after consuming
 * this stream must equal its final state after consuming the canonical
 * stream.
 */
public final class NoisyStreamBuilder {

    private NoisyStreamBuilder() {
    }

    public static List<CardMessage> buildNoisyStream(List<CardMessage> canonical, double duplicateFraction, long seed) {
        Random random = new Random(seed);
        List<CardMessage> withDuplicates = new ArrayList<>(canonical);

        int duplicateCount = (int) Math.round(canonical.size() * duplicateFraction);
        for (int i = 0; i < duplicateCount; i++) {
            CardMessage original = canonical.get(random.nextInt(canonical.size()));
            withDuplicates.add(original); // same (messageId, traceNumber): a genuine redelivery, not a new message
        }

        java.util.Collections.shuffle(withDuplicates, random);
        return withDuplicates;
    }
}
