package com.tdc.kafka.kafka_coffee;

import com.tdc.kafka.kafka_coffee.model.Item;
import com.tdc.kafka.kafka_coffee.model.Order;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@SuppressWarnings({"ResultOfMethodCallIgnored", "BigDecimalMethodWithoutRoundingCalled"})
public class UserInputEventStream extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(UserInputEventStream.class);

    @Override
    public Completable rxStart() {
        JsonObject externalKafkaConfig = config().getJsonObject("kafka");

        Map<String, String> prodConfig = new HashMap<>();
        prodConfig.put("bootstrap.servers", externalKafkaConfig.getString("bootstrap.servers"));
        prodConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodConfig.put("acks", "1");

        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, prodConfig);

        vertx.periodicStream(10000).toFlowable()
                .subscribe(timeout -> {
                    Order order = createOrder(new Random().nextInt(9) + 1);
                    LOG.info("Generating order {}...", order.getId());
                    KafkaProducerRecord<String, String> record =
                            KafkaProducerRecord.create(externalKafkaConfig.getString("order.submitted.topic"), order.getId(), JsonObject.mapFrom(order).encode());
                    producer.rxWrite(record)
                            .subscribe(recordCreated -> LOG.info("Order submission {} event created...", order.getId()),
                                    Throwable::printStackTrace);
                });

        return Completable.complete();
    }

    protected Order createOrder(int numItems) {
        Order order = new Order();

        Observable.range(0, numItems)
                .forEach(index -> {
                    Item item1 = new Item();
                    item1.setProdId("1");
                    item1.setValue(BigDecimal.TEN.setScale(2));
                    item1.setAmount(1);
                    order.addItem(item1);
                });

        return order;
    }
}
