package com.tdc.kafka.kafka_coffee;

import com.tdc.kafka.kafka_coffee.model.Order;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("ResultOfMethodCallIgnored")
@ExtendWith(VertxExtension.class)
@DisplayName("Test Coffee Store Event Stream Verticle")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestEventStreamVerticle extends BaseTest {

    private Logger log = LoggerFactory.getLogger(TestEventStreamVerticle.class);

    @BeforeEach
    void deployVerticle(Vertx vertx, VertxTestContext testContext) {
        testContext.completeNow();
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Test Event Submission")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    void testTopic(Vertx vertx, VertxTestContext testContext) throws Throwable {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "192.168.99.2:9092, 192.168.99.2:9093, 192.168.99.2:9094");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "1");

        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);

        Order order = createOrder();

        KafkaProducerRecord<String, String> record =
                KafkaProducerRecord.create("test-order-submitted", JsonObject.mapFrom(order).encode());

        producer.rxWrite(record)
                .ignoreElement()
                .andThen(producer.rxWrite(record))
                .ignoreElement().subscribe(
                testContext::completeNow,
                testContext::failNow);
    }
}
