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
            // FIRENZE -> ROMA
        // oLat=43,7792500&oLon=11.2462600&dLat=41.8919300&dLon=12.5113300
        Float oLat = Float.parseFloat(context.request().getParam("oLat"));
        Float oLon =  Float.parseFloat(context.request().getParam("oLon"));
        Float dLat =  Float.parseFloat(context.request().getParam("dLat"));
        Float dLon =  Float.parseFloat(context.request().getParam("dLon"));

        // Create origin and destination geometry points
        Map<String, Object> parameters = new HashMap<>();
                    parameters.put("oLat", oLat);
                    parameters.put("oLon", oLon);
                    parameters.put("dLat", dLat);
                    parameters.put("dLon", dLon);

         //BETWEEN (#{input_distance} - 10) AND (#{input_distance} + 10)
         // AND ST_DistanceSphere(#{destPoint}, #{originPoint}) < 10000")
         String input_Ogeo= "ST_SetSRID(ST_MakePoint(#{oLon} , #{oLat}), 4326)";
         String input_Dgeo= "ST_SetSRID(ST_MakePoint(#{dLon} , #{dLat}), 4326)";
         String distance = "ST_DistanceSphere("+ input_Ogeo + "," + input_Dgeo + ")"; 
         
         String query= "SELECT *, ST_DistanceSphere(o.origin_geom, o.destination_geom) as dist from schema.tratta o where ST_DistanceSphere(o.origin_geom, o.destination_geom) BETWEEN "+distance+"-100 AND "+distance+"+100";
         
         SqlTemplate.forQuery(db,query)
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
                    .put("dist", row.getValue("dist"))    
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
                }).subscribe(succ->{},err -> {
                LOG.debug("Failure2: ", err , err.getMessage());});
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
    
    private void getDistance(RoutingContext context){
            // FIRENZE -> ROMA
        // oLat="43,7792500"&oLon="11.2462600"&dLat="41.8919300"&dLon="2.5113300"
        Float oLat = Float.parseFloat(context.request().getParam("oLat"));
        Float oLon =  Float.parseFloat(context.request().getParam("oLon"));
        Float dLat =  Float.parseFloat(context.request().getParam("dLat"));
        Float dLon =  Float.parseFloat(context.request().getParam("dLon"));

        // Create origin and destination geometry points
        Map<String, Object> parameters = new HashMap<>();
                    parameters.put("oLat", oLat);
                    parameters.put("oLon", oLon);
                    parameters.put("dLat", dLat);
                    parameters.put("dLon", dLon);

                    //BETWEEN (#{input_distance} - 10) AND (#{input_distance} + 10)
         // AND ST_DistanceSphere(#{destPoint}, #{originPoint}) < 10000")
         String input_Ogeo= "ST_SetSRID(ST_MakePoint(#{oLon} , #{oLat}), 4326)";
         String input_Dgeo= "ST_SetSRID(ST_MakePoint(#{dLon} , #{dLat}), 4326)";
         String distance = "ST_DistanceSphere("+input_Ogeo+ "," + input_Dgeo + ")"; 
         String query= "SELECT " + distance +" as dist";
        SqlTemplate.forQuery(db,query)
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
                    .put("dist", row.getValue("dist"));    
                    response.add(rowJson);
                });
                    LOG.info("Path {} responds with {}", context.normalizedPath(), response.encode());
                    context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                    .end(response.encode());
                }).subscribe(succ->{},err -> {
                LOG.debug("Failure2: ", err , err.getMessage());});
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