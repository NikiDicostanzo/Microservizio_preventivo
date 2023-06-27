package com.caribu.filiale.operatorInf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caribu.filiale.VertxRxWeb;
import com.caribu.filiale.quotes;
import com.caribu.filiale.db.DbResponse;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava3.ext.web.RoutingContext;
import io.vertx.rxjava3.sqlclient.Pool;
import io.vertx.rxjava3.sqlclient.templates.SqlTemplate;

public class GetSameTratta implements Handler<RoutingContext> {

    private static final Logger LOG = LoggerFactory.getLogger(GetSameTratta.class);
    private final Pool db;

    public GetSameTratta(final Pool db) {
        this.db = db;
    }

    @Override
    public void handle(final RoutingContext context) {
        // in input ho origine=(LAT, LONG), destinazione=(LAT, LONG)
        String origine = context.request().getParam("origine");
        String destinazione = context.request().getParam("destinazione");
        LOG.debug("Tratta : {}=>{}", origine, destinazione);
        getTratta(context);
        //String destinazione = context.pathParam("destinazione");
//         if(origine == null){
//             LOG.info("Executing DB query to find all users...");
//             getAllTratta(context);
//         }
//         else{
//            getTratta(context);
//         }
//    
}


    private void getTratta(RoutingContext context){
        // String longitudine = context.request().getParam("longitudine");
        // String latitudine = context.request().getParam("latitudine");
        // String id_tratta = context.request().getParam("id_tratta");
        String oLat = context.request().getParam("oLat");
        String oLon = context.request().getParam("oLon");
        String dLat = context.request().getParam("dLat");
        String dLon = context.request().getParam("dLon");


        Map<String, Object> parameters = new HashMap<>();
                    parameters.put("oLat", oLat);
                    parameters.put("oLon", oLon);
                    parameters.put("dLat", dLat);
                    parameters.put("dLon", dLon);

                    // parameters.put("id_tratta", id_tratta);
                    // parameters.put("longitudine", longitudine);
                    // parameters.put("latitudine", latitudine);
        // Create origin and destination geometry points
        String originPoint = "ST_SetSRID(ST_MakePoint(" + oLon + ", " + oLat + "), 4326)";
        String destPoint = "ST_SetSRID(ST_MakePoint(" + dLon + ", " + dLat + "), 4326)";
         // AND ST_DistanceSphere(#{destPoint}, #{originPoint}) < 10000")
        SqlTemplate.forQuery(db,
            "SELECT * from schema.tratta o where ST_DistanceSphere(o.origin_geom, o.destination_geom) > 10")
            .rxExecute(parameters)  //TODO
            .doOnError(err -> {
                LOG.debug("Failure: ", err , err.getMessage());
                context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                    .setStatusCode(500)
                    .end(new JsonObject().put("error", err.getMessage()).encode());
            })
            .doOnSuccess(result -> {
                //TODO errore nome sbagliato
                LOG.info("Got " + result.size() + " rows ");
                JsonArray response = new JsonArray();
                result.forEach(row -> {
                    JsonObject rowJson = new JsonObject()
                    .put("id_tratta", row.getValue("id_tratta"))
                    .put("latitudine", row.getValue("latitudine"))
                    .put("longitudine", row.getValue("longitudine"))
                    .put("origin_geom", row.getValue("origin_geom"))
                    .put("destination_geom", row.getValue("destination_geom"));
                    response.add(rowJson);
                });
                    LOG.info("Path {} responds with {}", context.normalizedPath(), response.encode());
                    context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                    .end(response.encode());
                }).subscribe();
        }

    private void getAllTratta(RoutingContext context){
        db.query("SELECT * FROM schema.tratta")
            .rxExecute()
            .doOnSuccess(result -> {
            LOG.info("Got " + result.size() + " rows ");
            JsonArray response = new JsonArray();
            result.forEach(row -> {
                JsonObject rowJson = new JsonObject()
                .put("id_tratta", row.getValue("id_tratta"))
                .put("latitudine", row.getValue("latitudine"))
                .put("longitudine", row.getValue("longitudine"));
                response.add(rowJson);
            });
                LOG.info("Path {} responds with {}", context.normalizedPath(), response.encode());
                context.response()
                .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                .end(response.encode());
            })
            .doOnError(err -> {
            LOG.debug("Failure: ", err , err.getMessage());
            context.response()
                .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                .setStatusCode(500)
                .end(new JsonObject().put("error", err.getMessage()).encode());
            })
            .subscribe(sr->{},err->{LOG.debug("FailureSbu: ", err , err.getMessage());
                }); // Don't forget to subscribe to the Single
            }
}

    

// //TODO creare funzionE!!!
//   JsonArray response = new JsonArray();
//             result.forEach(row -> {
//                 JsonObject rowJson = new JsonObject()
//                 .put("id", row.getValue("id"))
//                 .put("origine", row.getValue("origine"))
//                 .put("destinazione", row.getValue("destinazione"))
//                 .put("km", row.getValue("km"));
//                 response.add(rowJson);