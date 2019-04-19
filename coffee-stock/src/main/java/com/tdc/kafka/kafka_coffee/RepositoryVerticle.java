package com.tdc.kafka.kafka_coffee;

import com.sun.istack.internal.NotNull;
import com.tdc.kafka.kafka_coffee.model.Order;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.asyncsql.MySQLClient;
import io.vertx.reactivex.ext.sql.SQLClient;
import io.vertx.reactivex.ext.sql.SQLClientHelper;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

@SuppressWarnings({"ResultOfMethodCallIgnored", "Duplicates"})
public class RepositoryVerticle extends AbstractVerticle {

    public static final String STOCK_UPDATE_QUEUE = "stock-update";
    public static final String REFILL_STOCK_QUEUE = "refill-stock";

    private final String UPDATE_STOCK;
    private final String REFILL_STOCK_1_2;
    private final String REFILL_STOCK_3;
    private static final Logger LOG = LoggerFactory.getLogger(RepositoryVerticle.class);
    private static final String QUERIES_PROPERTIES_FILE = "/queries.properties";
    private static final String TABLES_SQL_FILE = "/tables.sql";
    private SQLClient mySQLClient;

    public RepositoryVerticle() {
        super();
        try {
            Properties properties = new Properties();
            properties.load(RepositoryVerticle.class.getResourceAsStream(QUERIES_PROPERTIES_FILE));
            UPDATE_STOCK = properties.getProperty("update.stock");
            REFILL_STOCK_1_2 = properties.getProperty("refill.update.stock.1.2");
            REFILL_STOCK_3 = properties.getProperty("refill.update.stock.3");
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Completable rxStart() {
        JsonObject mySQLClientConfig = config().getJsonObject("mysql");

        mySQLClient = MySQLClient.createShared(vertx, mySQLClientConfig);

        try {
            registerStockUpdate();
            refillStockUpdate();

            List<String> tableStatements = getTablesStatements();

            List<Completable> completables = new ArrayList<>();

            Observable.fromIterable(tableStatements)
                    .forEach(tableStatement ->
                            completables.add(updateDB(mySQLClient, tableStatement)));

            return Completable.concatArray(completables.toArray(new Completable[0]));
        } catch (Exception ex) {
            ex.printStackTrace();
            return Completable.error(ex);
        }
    }

    private void registerStockUpdate() {
        vertx.eventBus().<String>consumer(STOCK_UPDATE_QUEUE)
                .toFlowable()
                .subscribe(message -> {
                    Order order = new JsonObject(message.body()).mapTo(Order.class);

                    List<Completable> completables = new ArrayList<>();

                    mySQLClient.rxGetConnection().subscribe(conn -> {
                        Observable.fromIterable(order.getItems())
                                .forEach(item -> completables.add(
                                        conn.rxUpdateWithParams(UPDATE_STOCK, new JsonArray(
                                                Arrays.asList(item.getAmount(),
                                                        item.getProdId(),
                                                        item.getAmount())))
                                                .doOnSuccess(updateResult -> {
                                                    if (updateResult.getUpdated() == 0) {
                                                        throw new OutOfStockException(String.format("Product %s not found or out of stock!", item.getProdId()), order);
                                                    }
                                                })
                                                .ignoreElement()));

                        Completable.concatArray(completables.toArray(new Completable[0]))
                                .compose(SQLClientHelper.txCompletableTransformer(conn))
                                .doFinally(conn::close)
                                .subscribe(() -> message.reply(buildReply(order, null)),
                                        error -> message.reply(buildReply(order, error)));
                    }, error -> message.fail(1, error.getMessage()));
                }, error -> LOG.error(error.getMessage(), error));
    }

    private void refillStockUpdate() {
        vertx.eventBus().<String>consumer(REFILL_STOCK_QUEUE)
                .toFlowable()
                .subscribe(message -> {
                    mySQLClient.rxGetConnection().subscribe(conn -> {
                        conn.rxUpdate(REFILL_STOCK_1_2)
                                .ignoreElement()
                                .andThen(conn.rxUpdate(REFILL_STOCK_3)
                                        .ignoreElement())
                                .compose(SQLClientHelper.txCompletableTransformer(conn))
                                .doFinally(conn::close)
                                .subscribe(() -> message.reply(true),
                                        error -> message.fail(1, error.getMessage()));
                    });
                }, error -> LOG.error(error.getMessage(), error));
    }

    private String buildReply(Order order, Throwable error) {
        return new JsonObject()
                .put("orderId", order.getId())
                .put("success", error == null)
                .put("outOfStockError", error instanceof OutOfStockException)
                .encode();
    }

    private Completable updateDB(@NotNull SQLClient mySQLClient, @NotNull String tableStatement) {
        Function<String, String> calcLength = (str) -> {
            if (str.length() - 1 > 50) {
                return str.substring(0, 50);
            } else {
                return str.substring(0, str.length() - 1);
            }
        };

        return mySQLClient.rxUpdate(tableStatement)
                .doOnSuccess(updateResult -> LOG.info("statement executed '{}...'", calcLength.apply(tableStatement)))
                .ignoreElement();
    }

    private List<String> getTablesStatements() throws IOException {
        InputStream stream = RepositoryVerticle.class.getResourceAsStream(TABLES_SQL_FILE);
        return IOUtils.readLines(stream, "utf8");
    }

}
