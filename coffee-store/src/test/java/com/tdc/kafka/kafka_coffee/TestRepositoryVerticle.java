package com.tdc.kafka.kafka_coffee;

import com.tdc.kafka.kafka_coffee.model.Item;
import com.tdc.kafka.kafka_coffee.model.Order;
import com.tdc.kafka.kafka_coffee.model.OrderStatus;
import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.ext.asyncsql.MySQLClient;
import io.vertx.reactivex.ext.sql.SQLClient;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SuppressWarnings("ResultOfMethodCallIgnored")
@ExtendWith(VertxExtension.class)
@DisplayName("Test Coffee Store Reposiory Verticle")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestRepositoryVerticle extends BaseTest {

    private Logger log = LoggerFactory.getLogger(TestRepositoryVerticle.class);

    @BeforeEach
    void deployVerticle(Vertx vertx, VertxTestContext testContext) {
        DeploymentOptions options = new DeploymentOptions()
                .setConfig(new JsonObject().put("mysql",
                        new JsonObject().put("host", "192.168.99.2")
                                .put("username", "user")
                                .put("password", "test123")
                                .put("database", "coffee_store")));
        vertx.rxDeployVerticle(new RepositoryVerticle(), options).subscribe(
                id -> {
                    log.info("Started with id {}", id);
                    testContext.completeNow();
                });
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Test Database Tables")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    void testStartDatabase(Vertx vertx, VertxTestContext testContext) throws Throwable {
        JsonObject mySQLClientConfig = new JsonObject()
                .put("host", "192.168.99.2")
                .put("username", "user")
                .put("password", "test123")
                .put("database", "coffee_store");
        SQLClient mySQLClient = MySQLClient.createShared(vertx, mySQLClientConfig);

        mySQLClient.rxQuery("SELECT * FROM item;")
                .subscribe(res -> {
                    testContext.verify(() -> {
                        log.info("Size: {}", res.getRows().size());
                        testContext.completeNow();
                    });
                }, ex -> {
                    ex.printStackTrace();
                    testContext.failNow(ex);
                });
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Test Update Order")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    void testInsertOrder(Vertx vertx, VertxTestContext testContext) throws Throwable {
        Order order = createOrder();
        saveOrder(vertx, testContext, order)
                .subscribe(reply -> testContext.verify(() -> {
                            validateOrder(testContext, order, reply);
                        }),
                        testContext::failNow);
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Test Get Order")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    void testGetOrder(Vertx vertx, VertxTestContext testContext) throws Throwable {
        Order order = createOrder();
        saveOrder(vertx, testContext, order)
                .subscribe(reply1 -> {
                    Order received1 = new JsonObject(reply1.body()).mapTo(Order.class);
                    vertx.eventBus().<String>rxSend(RepositoryVerticle.GET_ORDER_QUEUE, new JsonObject().put("id", received1.getId()).encode())
                            .subscribe(reply -> testContext.verify(() -> {
                                        validateOrder(testContext, order, reply);
                                    }),
                                    testContext::failNow);
                });
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Test Update Order Status")
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    void testUpdateOrderStatus(Vertx vertx, VertxTestContext testContext) throws Throwable {
        Order order = createOrder();
        saveOrder(vertx, testContext, order)
                .subscribe(reply1 -> {
                    Order received1 = new JsonObject(reply1.body()).mapTo(Order.class);
                    received1.setPlacedDate(new Date());
                    received1.setConfirmedDate(new Date());
                    received1.setDeliveredDate(new Date());
                    received1.setStatus(OrderStatus.DELIVERED);
                    vertx.eventBus().<String>rxSend(RepositoryVerticle.ORDER_UPDATE_QUEUE, JsonObject.mapFrom(received1).encode())
                            .subscribe(reply2 -> testContext.verify(() -> {
                                        validateOrder(testContext, received1, reply2);
                                    }),
                                    testContext::failNow);
                });
    }

    private void validateOrder(VertxTestContext testContext, Order order, Message<String> reply) {
        Order received = new JsonObject(reply.body()).mapTo(Order.class);

        assertEquals(order.getId(), received.getId(), "Id is not equal!");
        assertNotNull(received.getStatus(), "Status should not be null");

        switch (received.getStatus()) {
            case DELIVERED:
                assertNotNull(received.getPlacedDate(), "Placed Date should not be null!");
                assertNotNull(received.getConfirmedDate(), "Confirmed Date should not be null!");
                assertNotNull(received.getDeliveredDate(), "Delivered Date should not be null!");
                break;
            case CONFIRMED:
                assertNotNull(received.getPlacedDate(), "Placed Date should not be null!");
                assertNotNull(received.getConfirmedDate(), "Confirmed Date should not be null!");
                break;
            case CANCELED:
                assertNotNull(received.getPlacedDate(), "Placed Date should not be null!");
                assertNotNull(received.getCanceledDate(), "Canceled Date should not be null!");
                break;
            default:
                assertNotNull(received.getPlacedDate(), "Placed Date should not be null!");
                break;
        }
        l1:
        for (Item expectedItem : order.getItems()) {
            for (Item receivedItem : received.getItems()) {
                if (expectedItem.getId().equals(receivedItem.getId())) {
                    assertEquals(expectedItem.getAmount(), receivedItem.getAmount(), "Amount is not equal!");
                    assertEquals(expectedItem.getProdId(), receivedItem.getProdId(), "ProdId is not equal!");
                    assertEquals(expectedItem.getValue().setScale(2), receivedItem.getValue().setScale(2), "Value is not equal!");
                    continue l1;
                }
            }
            testContext.failNow(new Exception("Item not found"));
        }
        testContext.completeNow();
    }

    private Single<Message<String>> saveOrder(Vertx vertx, VertxTestContext testContext, Order order) {
        return vertx.eventBus().rxSend(RepositoryVerticle.ORDER_UPDATE_QUEUE, JsonObject.mapFrom(order).encode());
    }

}
