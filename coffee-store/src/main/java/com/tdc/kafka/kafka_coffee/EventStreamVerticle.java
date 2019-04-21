package com.tdc.kafka.kafka_coffee;

import com.tdc.kafka.kafka_coffee.model.Order;
import com.tdc.kafka.kafka_coffee.model.OrderStatus;
import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Process events from Kafka
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class EventStreamVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamVerticle.class);

    @Override
    public Completable rxStart() {
        return Completable.fromAction(() -> {
            JsonObject externalKafkaConfig = config().getJsonObject("kafka");

            Map<String, String> consConfig = new HashMap<>();
            consConfig.put("bootstrap.servers", externalKafkaConfig.getString("bootstrap.servers"));
            consConfig.put("group.id", externalKafkaConfig.getString("group.id"));
            consConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consConfig.put("auto.offset.reset", "earliest");
            consConfig.put("enable.auto.commit", "false");

            KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consConfig);

            Map<String, String> prodConfig = new HashMap<>();
            prodConfig.put("bootstrap.servers", externalKafkaConfig.getString("bootstrap.servers"));
            prodConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            prodConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            prodConfig.put("acks", "1");

            KafkaProducer<String, String> producer = KafkaProducer.create(vertx, prodConfig);

            consumer.subscribe(
                    new HashSet<>(Arrays.asList(
                            externalKafkaConfig.getString("order.submitted.topic"),
                            externalKafkaConfig.getString("order.confirmed.topic"),
                            externalKafkaConfig.getString("order.preparation.started.topic"),
                            externalKafkaConfig.getString("order.preparation.finished.topic"),
                            externalKafkaConfig.getString("order.canceled.topic")
                    ))
            );

            consumer.toObservable()
                    .subscribe(
                            record -> vertx.eventBus().<String>rxSend(RepositoryVerticle.ORDER_UPDATE_QUEUE, record.value())
                                    .subscribe(message -> {
                                                Order order = new JsonObject(message.body()).mapTo(Order.class);
                                                LOG.info("Order {} saved to database...", order.getId());

                                                if (order.getStatus() == OrderStatus.PLACED) {
                                                    KafkaProducerRecord<String, String> newRecord =
                                                            KafkaProducerRecord.create(externalKafkaConfig.getString("order.placed.topic"),
                                                                    order.getId(), JsonObject.mapFrom(order).encode());
                                                    //Generating a new event for a new placed order
                                                    producer.rxWrite(newRecord)
                                                            .subscribe(recordCreated -> LOG.info("Order {} {} event created...",
                                                                    order.getId(), order.getStatus()),
                                                                    Throwable::printStackTrace);
                                                    LOG.info("Event processed {}", message.body());
                                                }

                                                consumer.commit();
                                            },
                                            error -> LOG.error(error.getMessage(), error.getCause())));

            LOG.info(">> Coffee Store - Event Stream Consumer Started!");
        });
    }
}
