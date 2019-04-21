package com.tdc.kafka.kafka_coffee;

import com.tdc.kafka.kafka_coffee.model.Order;
import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.tdc.kafka.kafka_coffee.RepositoryVerticle.GET_PRODUCTS;
import static com.tdc.kafka.kafka_coffee.RepositoryVerticle.STOCK_UPDATED;

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
                    new HashSet<>(Collections.singletonList(
                            externalKafkaConfig.getString("order.placed.topic")
                    ))
            );

            consumer.toObservable()
                    .subscribe(
                            record -> vertx.eventBus().<String>rxSend(RepositoryVerticle.STOCK_UPDATE_QUEUE, record.value())
                                    .subscribe(message -> {
                                                try {
                                                    Order order = new JsonObject(record.value()).mapTo(Order.class);
                                                    JsonObject reply = new JsonObject(message.body());

                                                    KafkaProducerRecord<String, String> newRecord;

                                                    if (reply.getBoolean("success")) {
                                                        order.setConfirmedDate(new Date());
                                                        order.validateStatus();
                                                        LOG.info("Order {} items stock withdrew...", order.getId());
                                                        newRecord =
                                                                KafkaProducerRecord.create(externalKafkaConfig.getString("order.confirmed.topic"),
                                                                        order.getId(), JsonObject.mapFrom(order).encode());
                                                    } else {
                                                        order.setCanceledDate(new Date());
                                                        order.validateStatus();
                                                        LOG.info("Order {} canceled...", order.getId());
                                                        newRecord =
                                                                KafkaProducerRecord.create(externalKafkaConfig.getString("order.canceled.topic"),
                                                                        order.getId(), JsonObject.mapFrom(order).encode());
                                                    }

                                                    //Generating a new event for a new placed order
                                                    producer.rxWrite(newRecord)
                                                            .subscribe(recordCreated -> LOG.info("Order {} {} event created...",
                                                                    order.getId(), order.getStatus()),
                                                                    Throwable::printStackTrace);

                                                    LOG.info("Event processed {}", message.body());
                                                    consumer.commit();
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                }
                                            },
                                            error -> LOG.error(error.getMessage(), error)),
                            error -> LOG.error(error.getMessage(), error));

            vertx.eventBus().consumer(STOCK_UPDATED).toFlowable()
                    .subscribe(event -> {
                        vertx.eventBus().<String>rxSend(GET_PRODUCTS, null)
                                .subscribe(reply -> {
                                    KafkaProducerRecord<String, String> newRecord =
                                            KafkaProducerRecord.create(externalKafkaConfig.getString("stock.updated.topic"),
                                                    reply.body());

                                    producer.rxWrite(newRecord)
                                            .subscribe(recordCreated -> LOG.info("Stock updated event created..."),
                                                    Throwable::printStackTrace);
                                });
                    });

            LOG.info(">> Coffee Store - Event Stream Consumer Started!");
        });
    }
}
