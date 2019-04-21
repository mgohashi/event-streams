package com.tdc.kafka.kafka_coffee;

import com.tdc.kafka.kafka_coffee.model.Item;
import com.tdc.kafka.kafka_coffee.model.Order;
import com.tdc.kafka.kafka_coffee.model.OrderStatus;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.asyncsql.MySQLClient;
import io.vertx.reactivex.ext.sql.SQLClient;
import io.vertx.reactivex.ext.sql.SQLClientHelper;
import io.vertx.reactivex.ext.sql.SQLConnection;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;

@SuppressWarnings({"ResultOfMethodCallIgnored", "Duplicates"})
public class RepositoryVerticle extends AbstractVerticle {

    public static final String ORDER_UPDATE_QUEUE = "order-update";
    public static final String GET_ORDER_QUEUE = "get-order";

    private final String INSERT_OR_UPDATE_ORDER;
    private final String INSERT_OR_UPDATE_ITEM;
    private final String QUERY_ORDER;
    private final String QUERY_ITEM_BY_ORDER_ID;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final Logger LOG = LoggerFactory.getLogger(RepositoryVerticle.class);
    private static final String QUERIES_PROPERTIES_FILE = "/queries.properties";
    private static final String TABLES_SQL_FILE = "/tables.sql";
    private SQLClient mySQLClient;

    public RepositoryVerticle() {
        super();
        try {
            Properties properties = new Properties();
            properties.load(RepositoryVerticle.class.getResourceAsStream(QUERIES_PROPERTIES_FILE));
            INSERT_OR_UPDATE_ORDER = properties.getProperty("insert.or.update.order");
            INSERT_OR_UPDATE_ITEM = properties.getProperty("insert.or.update.item");
            QUERY_ORDER = properties.getProperty("query.order");
            QUERY_ITEM_BY_ORDER_ID = properties.getProperty("query.item.by.order.id");
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
            registerInsertOrder();
            registerGetOrder();

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

    private void registerInsertOrder() {
        vertx.eventBus().<String>consumer(ORDER_UPDATE_QUEUE)
                .toFlowable()
                .subscribe(message -> {
                    Order order = new JsonObject(message.body()).mapTo(Order.class);

                    if (order.getPlacedDate() == null) {
                        order.setStatus(OrderStatus.PLACED);
                        order.setPlacedDate(java.util.Date.from(LocalDateTime.now()
                                .atZone(ZoneId.systemDefault()).toInstant()));
                    }

                    mySQLClient.rxGetConnection().subscribe(conn -> {
                        conn.rxUpdateWithParams(INSERT_OR_UPDATE_ORDER, new JsonArray(Arrays.asList(order.getId(),
                                order.getTotal(),
                                order.getPlacedDate(),
                                order.getConfirmedDate(),
                                order.getDeliveredDate(),
                                order.getCanceledDate()))).ignoreElement()
                                .andThen(updateItems(conn, order))
                                .compose(SQLClientHelper.txCompletableTransformer(conn))
                                .doFinally(conn::close)
                                .subscribe(() -> getOrderById(conn, order.getId())
                                                .subscribe(res -> message.reply(JsonObject.mapFrom(res).encode()),
                                                        error -> message.fail(1, error.getMessage())),
                                        error -> message.fail(1, error.getMessage()));
                    });
                });
    }

    private void registerGetOrder() {
        vertx.eventBus().<String>consumer(GET_ORDER_QUEUE)
                .toFlowable()
                .subscribe(message -> {
                    Order order = new JsonObject(message.body()).mapTo(Order.class);
                    order.setPlacedDate(java.util.Date.from(LocalDateTime.now()
                            .atZone(ZoneId.systemDefault()).toInstant()));
                    mySQLClient.rxGetConnection().subscribe(conn ->
                            getOrderById(conn, order.getId())
                                    .doFinally(conn::close)
                                    .subscribe(res ->
                                                    message.reply(JsonObject.mapFrom(res).encode()),
                                            error -> message.fail(1, error.getMessage())));
                });
    }

    private Single<Order> getOrderById(SQLConnection conn, String orderId) {
        return conn.rxQuerySingleWithParams(QUERY_ORDER, new JsonArray(Collections.singletonList(orderId)))
                .map(res -> new Order(res.getString(0),
                        new BigDecimal(res.getString(1)).setScale(2),
                        parseDateValue(res.getString(2)),
                        parseDateValue(res.getString(3)),
                        parseDateValue(res.getString(4)),
                        parseDateValue(res.getString(5))))
                .flatMapSingle(order ->
                        conn.rxQueryWithParams(QUERY_ITEM_BY_ORDER_ID, new JsonArray(Collections.singletonList(orderId)))
                                .map(ResultSet::getResults)
                                .flatMap(rows -> {
                                            rows.forEach(
                                                    res -> order.addItem(
                                                            new Item(res.getString(0),
                                                                    res.getString(1),
                                                                    res.getInteger(2),
                                                                    new BigDecimal(res.getString(3))))
                                            );
                                            return Single.just(order);
                                        }
                                ));
    }

    private Completable updateDB(SQLClient mySQLClient, String tableStatement) {
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

    private Date parseDateValue(String string) throws ParseException {
        if (string != null)
            return DATE_FORMAT.parse(string);
        return null;
    }

    private Completable updateItems(SQLConnection conn, Order order) {
        if (order.getStatus() != OrderStatus.PLACED) {
            return Completable.complete();
        }

        List<Completable> completables = new ArrayList<>();

        Observable.fromIterable(order.getItems())
                .forEach(item -> completables.add(
                        conn.rxUpdateWithParams(INSERT_OR_UPDATE_ITEM,
                                new JsonArray(Arrays.asList(
                                        item.getId(),
                                        item.getProdId(),
                                        order.getId(),
                                        item.getAmount(),
                                        item.getValue()))).ignoreElement()));

        return Completable.concatArray(completables.toArray(new Completable[0]));
    }
}
