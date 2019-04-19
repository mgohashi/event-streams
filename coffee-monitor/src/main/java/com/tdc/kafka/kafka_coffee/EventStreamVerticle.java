package com.tdc.kafka.kafka_coffee;

import com.tdc.kafka.kafka_coffee.model.Order;
import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.handler.StaticHandler;
import io.vertx.reactivex.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process events from Kafka
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class EventStreamVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamVerticle.class);
    private AtomicLong placed = new AtomicLong();
    private AtomicLong deliveries = new AtomicLong();
    private AtomicLong cancelled = new AtomicLong();
    private DecimalFormat decimalFormat = new DecimalFormat();

    @Override
    public Completable rxStart() {
        decimalFormat.setMaximumFractionDigits(2);

        registerReadingEvents();

        Router router = Router.router(vertx);
        router.get("/*").handler(StaticHandler.create().setCachingEnabled(false));
        router.get("/").handler(context -> context.reroute("app/index.html"));
        router.route("/assets/lib/*").handler(StaticHandler.create("META-INF/resources/webjars"));

        SockJSHandler sockJSHandler = SockJSHandler.create(vertx);
        BridgeOptions options = new BridgeOptions();
        options.addInboundPermitted(new PermittedOptions().setAddressRegex("public-.*"));
        options.addOutboundPermitted(new PermittedOptions().setAddressRegex("public-.*"));
        sockJSHandler.bridge(options);

        router.route("/eventbus/*").handler(sockJSHandler);

        return vertx.createHttpServer().requestHandler(router).rxListen(8080, "0.0.0.0")
                .doOnSuccess(httpServer ->
                        LOG.info("Listening in localhost:{}...", httpServer.actualPort()))
                .ignoreElement();
    }

    private void registerReadingEvents() {
        JsonObject externalKafkaConfig = config().getJsonObject("kafka");

        Map<String, String> consConfig = new HashMap<>();
        consConfig.put("bootstrap.servers", externalKafkaConfig.getString("bootstrap.servers"));
        consConfig.put("group.id", externalKafkaConfig.getString("group.id"));
        consConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consConfig.put("auto.offset.reset", "earliest");
        consConfig.put("enable.auto.commit", "false");

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consConfig);

        consumer.subscribe(new HashSet<>(Arrays.asList(
                externalKafkaConfig.getString("order.placed.topic"),
                externalKafkaConfig.getString("order.preparation.finished.topic"),
                externalKafkaConfig.getString("order.cancelled.topic"))));

        consumer.toObservable()
                .subscribe(
                        record -> {
                            Order order = new JsonObject(record.value()).mapTo(Order.class);
                            updateCountersAndPublish(order);
                            consumer.commit();
                        });

        KafkaConsumer<String, String> stockUpdatedConsumer = KafkaConsumer.create(vertx, consConfig);
        stockUpdatedConsumer.subscribe(externalKafkaConfig.getString("stock.updated.topic"));
        stockUpdatedConsumer.toObservable()
                .subscribe(record -> {
                    String event = record.value();
                    String topic = "public-product-count";
                    publishEvent(topic, event);
                });
    }

    private void updateCountersAndPublish(Order order) {
        switch (order.getStatus()) {
            case PLACED: {
                placed.incrementAndGet();
                break;
            }
            case DELIVERED: {
                deliveries.incrementAndGet();
                break;
            }
            case CANCELLED: {
                cancelled.incrementAndGet();
                break;
            }
        }

        try {
            updatePlaced();
            updateDelivered();
            updateCancelled();
        } catch (ArithmeticException e) {
            e.printStackTrace();
        }
    }

    private void updatePlaced() {
        String event = "{\"count\":" + placed.get() + "}";
        String topic = "public-orders-placed";
        publishEvent(topic, event);
    }

    private void updateDelivered() {
        BigDecimal val = BigDecimal.ZERO;
        if (placed.get() > 0) {
            val = BigDecimal.valueOf(deliveries.get())
                    .divide(BigDecimal.valueOf(placed.get()), 2, RoundingMode.HALF_UP);
        }
        LOG.info("Deliveries: {}/{} = {}", deliveries.get(), placed.get(), val);
        String event = "{\"percent\":" + (decimalFormat.format(val.multiply(BigDecimal.valueOf(100)))) + "}";
        String topic = "public-orders-delivered";
        publishEvent(topic, event);
    }

    private void updateCancelled() {
        BigDecimal val = BigDecimal.ZERO;
        if (placed.get() > 0) {
            val = BigDecimal.valueOf(cancelled.get())
                    .divide(BigDecimal.valueOf(placed.get()),2, RoundingMode.HALF_UP);
        }
        LOG.info("Cancelled: {}/{} = {}", cancelled.get(), placed.get(), val);
        String event = "{\"percent\":" + (decimalFormat.format(val.multiply(BigDecimal.valueOf(100)))) + "}";
        String topic = "public-orders-cancelled";
        publishEvent(topic, event);
    }

    private void publishEvent(String topic, String event) {
        LOG.info("publishing event {} to {}...", event, topic);
        vertx.eventBus().publish(topic, event);
    }
}
