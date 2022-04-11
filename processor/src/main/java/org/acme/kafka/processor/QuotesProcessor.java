package org.acme.kafka.processor;

import java.util.Random;

import javax.enterprise.context.ApplicationScoped;

import org.acme.kafka.model.Quote;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.annotations.Blocking;

/**
 * A bean consuming data from the "quote-requests" Kafka topic (mapped to "requests" channel) and giving out a random quote.
 * The result is pushed to the "quotes" Kafka topic.
 */
@ApplicationScoped
public class QuotesProcessor {

    private Random random = new Random();

    @Blocking
    @Incoming("requests")
    @Outgoing("quotes")
    public Quote process(String quoteRequest) throws InterruptedException {
        Thread.sleep(200);
        int price = random.nextInt(100);

        if (price < 20) {
            throw new RuntimeException("Price to low!");
        }
        return new Quote(quoteRequest, price);
    }
}
