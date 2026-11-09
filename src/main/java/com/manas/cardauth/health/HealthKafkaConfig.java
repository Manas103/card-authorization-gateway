package com.manas.cardauth.health;

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
 * Kafka wiring for the health monitor's own topic, separate from
 * {@code KafkaConfig}'s ledger-engine topic: the health monitor and the
 * ledger engine are two independent consumers of the same conceptual
 * authorization/reversal/clearing stream, modeled here as two topics
 * because a real health-monitoring consumer group would not share a
 * container factory keyed to the ledger engine's own message type.
 */
@Configuration
public class HealthKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${cardauth.health.kafka.group-id:card-authorization-health-monitor}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, AuthEvent> authEventConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // AuthEventJson.MAPPER directly, not a Spring-managed ObjectMapper bean: a second
        // app-wide ObjectMapper bean (alongside KafkaConfig's cardMessageObjectMapper) would
        // make Spring Boot's own HttpMessageConverters autoconfiguration ambiguous, since it
        // autowires "the" ObjectMapper without a qualifier.
        JsonDeserializer<AuthEvent> valueDeserializer = new JsonDeserializer<>(AuthEvent.class, AuthEventJson.MAPPER);
        valueDeserializer.addTrustedPackages("com.manas.cardauth.health");
        valueDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AuthEvent> healthKafkaListenerContainerFactory(
            ConsumerFactory<String, AuthEvent> authEventConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, AuthEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(authEventConsumerFactory);
        return factory;
    }

    @Bean
    public ProducerFactory<String, AuthEvent> authEventProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JsonSerializer<>(AuthEventJson.MAPPER));
    }

    @Bean
    public KafkaTemplate<String, AuthEvent> authEventKafkaTemplate(ProducerFactory<String, AuthEvent> authEventProducerFactory) {
        return new KafkaTemplate<>(authEventProducerFactory);
    }
}
