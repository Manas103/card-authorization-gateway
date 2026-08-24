package com.manas.cardauth.kafka;

import com.manas.cardauth.model.CardMessage;
import com.manas.cardauth.service.CardAuthorizationService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes authorization, reversal and clearing messages off the
 * configured Kafka topic and hands each one to
 * {@link CardAuthorizationService}. This is the only place a raw Kafka
 * record becomes a balance decision; everything else is plain,
 * Kafka-unaware Java.
 */
@Component
public class CardMessageListener {

    private final CardAuthorizationService service;

    public CardMessageListener(CardAuthorizationService service) {
        this.service = service;
    }

    @KafkaListener(
            topics = "${cardauth.kafka.topic:card-authorization-messages}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(CardMessage message) {
        service.process(message);
    }
}
