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
             
        //String destinazione = context.pathParam("destinazione");
        String oLat = context.request().getParam("oLat");
        String oLon = context.request().getParam("oLon");
        String dLat =  context.request().getParam("dLat");
        String dLon = context.request().getParam("dLon");
        LOG.debug("Tratta : {},{}=>{},{}", oLat,oLon,dLat,dLon);
        //getDistance(context);
        if(oLat == null || oLon == null || dLat == null || dLon == null){
            // Get all tratta
            LOG.info("Executing DB query to find all tratta...");
            String query= "SELECT *,  ST_DistanceSphere(o.origin_geom, o.destination_geom) as dist FROM schema.quotes o";
            setQuery(context, query, null);;
        }
        else{
           getTratta(context, oLat, oLon, dLat, dLon);
        }
  
    }
    private void getTratta(RoutingContext context, String oLatS, String oLonS, String dLatS,String dLonS){
        // FIRENZE -> ROMA
        // oLat=43,7792500&oLon=11.2462600&dLat=41.8919300&dLon=12.5113300
        // FIRENZE->PISA
        // oLat=43.7792500&oLon=11.2462600&dLat=43.7085300&dLon=10.4036000
        Float oLat = Float.parseFloat(oLatS);
        Float oLon =  Float.parseFloat(oLonS);
        Float dLat =  Float.parseFloat(dLatS);
        Float dLon =  Float.parseFloat(dLonS);

        // Create origin and destination geometry points
        Map<String, Object> parameters = new HashMap<>();
                    parameters.put("oLat", oLat);
                    parameters.put("oLon", oLon);
                    parameters.put("dLat", dLat);
                    parameters.put("dLon", dLon);

         String input_Ogeo= "ST_SetSRID(ST_MakePoint(#{oLon} , #{oLat}), 4326)";
         String input_Dgeo= "ST_SetSRID(ST_MakePoint(#{dLon} , #{dLat}), 4326)";
         String distance = "ST_DistanceSphere("+ input_Ogeo + "," + input_Dgeo + ")"; 
         
         String query= "SELECT *, ST_DistanceSphere(o.origin_geom, o.destination_geom) as dist from schema.quotes o where ST_DistanceSphere(o.origin_geom, o.destination_geom) BETWEEN "+distance+"-10000 AND "+distance+"+10000";
         setQuery(context, query, parameters);
    }

    private void setQuery(RoutingContext context, String query, Map<String, Object> parameters){
            
        SqlTemplate.forQuery(db,query)
            .rxExecute(parameters)  
            .doOnError(err -> {
                LOG.debug("Failure: ", err , err.getMessage());
                context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                    .setStatusCode(500)
                    .end(new JsonObject().put("error", err.getMessage()).encode());
            })
            .doOnSuccess(result -> {
                LOG.info("Got " + result.size() + " rows ");
                JsonArray response = new JsonArray();
                result.forEach(row -> {
                    JsonObject rowJson = new JsonObject()
                    .put("dist", row.getValue("dist"))    
                    .put("id_quotes", row.getValue("id_quotes"))
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
