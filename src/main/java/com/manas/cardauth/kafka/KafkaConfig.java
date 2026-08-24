package com.manas.cardauth.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.manas.cardauth.model.CardMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka wiring built by hand (rather than off Spring Boot's
 * {@code KafkaProperties} auto-configuration) so the exact ObjectMapper
 * used for (de)serialization, including the {@code JavaTimeModule}
 * registration that {@code Instant}/amount handling needs, is explicit
 * and testable independent of which Spring Boot version's
 * {@code KafkaProperties} API is in play.
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:card-authorization-gateway}")
    private String groupId;

    @Bean
    public ObjectMapper cardMessageObjectMapper() {
        return CardMessageJson.MAPPER;
    }

    @Bean
    public ConsumerFactory<String, CardMessage> cardMessageConsumerFactory(ObjectMapper cardMessageObjectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        JsonDeserializer<CardMessage> valueDeserializer = new JsonDeserializer<>(CardMessage.class, cardMessageObjectMapper);
        valueDeserializer.addTrustedPackages("com.manas.cardauth.model");
        valueDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CardMessage> kafkaListenerContainerFactory(
            ConsumerFactory<String, CardMessage> cardMessageConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, CardMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cardMessageConsumerFactory);
        return factory;
    }

    @Bean
    public ProducerFactory<String, CardMessage> cardMessageProducerFactory(ObjectMapper cardMessageObjectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JsonSerializer<>(cardMessageObjectMapper));
    }

    @Bean
    public KafkaTemplate<String, CardMessage> cardMessageKafkaTemplate(ProducerFactory<String, CardMessage> cardMessageProducerFactory) {
        return new KafkaTemplate<>(cardMessageProducerFactory);
    }
}
