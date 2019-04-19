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

import java.math.BigDecimal;
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

}
