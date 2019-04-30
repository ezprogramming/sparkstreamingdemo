package com.streams.demo;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collections;

public class WebServer {

    public static String uberTopic;
    static int httpPort;

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            throw new IllegalArgumentException("Must have the HtttPort and Topic :  8080 /apps/ubertopic  ");
        }
        httpPort = Integer.parseInt(args[0]);
        uberTopic = args[1];

        Vertx vertx = Vertx.vertx();
        Router router = Router.router(vertx);
        BridgeOptions options = new BridgeOptions();
        options.setOutboundPermitted(Collections.singletonList(new PermittedOptions().setAddress("dashboard")));
        router.route("/eventbus/*").handler(SockJSHandler.create(vertx).bridge(options));
        router.route().handler(StaticHandler.create().setCachingEnabled(false));
        HttpServer httpServer = vertx.createHttpServer();
        httpServer.requestHandler(router::accept).listen(httpPort, ar -> {
            if (ar.succeeded()) {
                System.out.println("Http server started started on port " + httpPort);
            } else {
                ar.cause().printStackTrace();
            }
        });

       /* KafkaConsumer<String, String> consumer;
        Properties properties = new Properties();
        properties.setProperty("group", "vertx_dashboard");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(uberTopic));*/

      /*  System.out.println("consume from Kafka Topic " + uberTopic + " publish to eventbus dashboard");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            for (ConsumerRecord<String, String> record : records) {
                vertx.eventBus().publish("dashboard", record.value());
                System.out.println(record.value());
            }
        }*/
        File f = new File("/Users/david/bigdata/dataset/data/clust.txt");
        FileReader fr = new FileReader(f);
        BufferedReader reader = new BufferedReader(fr);
        String line = reader.readLine();
        while (line != null) {
            line = reader.readLine();
            vertx.eventBus().publish("dashboard", line);
            Thread.sleep(50);
        }
    }
}
