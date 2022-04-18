package org.acme.kafka.processor;

import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import org.acme.kafka.model.Quote;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.Random;

/**
 * A bean consuming data from the "quote-requests" Kafka topic (mapped to "requests" channel) and giving out a random quote.
 * The result is pushed to the "quotes" Kafka topic.
 */
@Startup
@ApplicationScoped
public class QuotesProcessor {

    private Random random = new Random();

    @Inject
    KafkaClientService clientService;

    @Inject
    @Identifier("default-kafka-broker")
    Map<String, Object> config;

    void onStartup(@Observes StartupEvent startupEvent) {
        System.out.println("Consumeing..." + config);
        KafkaConsumer<String, Void> consumer = clientService.getConsumer("requests");
        consumer.runOnPollingThread(client -> {
            ConsumerRecords<String, Void> records = client.poll(Duration.ofSeconds(5));
            while (!records.isEmpty()) {
                records = client.poll(Duration.ofSeconds(5));
                records.forEach(r -> {
                    int price = random.nextInt(100);
                    System.out.println(new Quote(r.key(), price));
                });
            }
        });
    }
}
