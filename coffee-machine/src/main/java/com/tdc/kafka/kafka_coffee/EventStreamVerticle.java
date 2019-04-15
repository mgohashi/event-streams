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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

            consumer.subscribe(externalKafkaConfig.getString("order.placed.topic"));

            consumer.toObservable()
                    .subscribe(
                            record -> {
                                Order order = new JsonObject(record.value()).mapTo(Order.class);

                                order.setConfirmedDate(java.util.Date.from(LocalDateTime.now()
                                        .atZone(ZoneId.systemDefault()).toInstant()));
                                order.setStatus(OrderStatus.CONFIRMED);

                                KafkaProducerRecord<String, String> newRecord1 =
                                        KafkaProducerRecord.create(externalKafkaConfig.getString("order.preparation.started.topic"),
                                                order.getId(), JsonObject.mapFrom(order).encode());
                                producer.rxWrite(newRecord1)
                                        .doOnSuccess(recordCreated -> LOG.info("Order {} preparation started event created...", order.getId()))
                                        .doOnError(Throwable::printStackTrace)
                                        .ignoreElement()
                                        .delay(5, TimeUnit.SECONDS)
                                        .subscribe(() -> {
                                            order.setDeliveredDate(java.util.Date.from(LocalDateTime.now()
                                                    .atZone(ZoneId.systemDefault()).toInstant()));
                                            order.setStatus(OrderStatus.DELIVERED);

                                            KafkaProducerRecord<String, String> newRecord2 =
                                                    KafkaProducerRecord.create(externalKafkaConfig.getString("order.preparation.finished.topic"),
                                                            order.getId(), JsonObject.mapFrom(order).encode());
                                            producer.rxWrite(newRecord2)
                                                    .subscribe(recordCreated -> LOG.info("Order {} preparation finished event created...", order.getId()),
                                                            Throwable::printStackTrace);
                                            LOG.info("Event processed {}", record.value());
                                            consumer.commit();
                                        });
                            });

            LOG.info("Event Stream Consumer Started!");
        });
    }
}
