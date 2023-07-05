package com.caribu.filiale.operatorInf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caribu.filiale.eliminare.WatchList;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.rxjava3.ext.web.RoutingContext;
import io.vertx.rxjava3.ext.web.validation.ValidationHandler;
import io.vertx.rxjava3.sqlclient.templates.SqlTemplate;
import io.vertx.rxjava3.sqlclient.Pool;
import io.vertx.rxjava3.sqlclient.Row;
import io.vertx.rxjava3.sqlclient.RowSet;
import io.vertx.rxjava3.sqlclient.Tuple;


public class AddQuotesCache implements Handler<RoutingContext> {

  private static final Logger LOG = LoggerFactory.getLogger(AddQuotesCache.class);
  private final Pool db;

  public AddQuotesCache(final Pool db) {
    this.db = db;
  }

  @Override
  public void handle(final RoutingContext context) {

    System.out.println("QUIIIIII");
    // in input ho un json
    RequestParameters params = context.get(ValidationHandler.REQUEST_CONTEXT_KEY); // (1)
 
    JsonObject json = params.body().getJsonObject(); // (2)
    System.out.println("json:  " + params.body());

    var watchList = json.mapTo(WatchList.class);
    var parameterBatch = watchList.getOp().stream()
      .map(op -> {
        final Map<String, Object> parameters = new HashMap<>();
        parameters.put("id_quotes", op.getId_quotes());
        parameters.put("id_commission", op.getId_commission());
        parameters.put("oLat", op.getOLat());
        parameters.put("oLon", op.getOLon());
        parameters.put("dLat", op.getDLat());
        parameters.put("dLon", op.getDLon());
        return parameters;
      }).collect(Collectors.toList());
      
      String input_Ogeo= "ST_SetSRID(ST_MakePoint(#{oLon} , #{oLat}), 4326)";
      String input_Dgeo= "ST_SetSRID(ST_MakePoint(#{dLon} , #{dLat}), 4326)";
      String distance = "ST_DistanceSphere("+ input_Ogeo + "," + input_Dgeo + ")"; 
      
      String query= "SELECT *, ST_DistanceSphere(o.origin_geom, o.destination_geom) as dist from schema.quotes o where ST_DistanceSphere(o.origin_geom, o.destination_geom) BETWEEN "+distance+"-10000 AND "+distance+"+10000";

      // Trovo valori con una distanza simile a distanza nella commessa
      SqlTemplate.forQuery(db,query)
            .rxExecuteBatch(parameterBatch)  
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
                    .put("id_commission", parameterBatch.get(0).get("id_commission"))    
                    .put("dist", row.getValue("dist"))    
                    .put("id_quotes", row.getValue("id_quotes"))
                    .put("origin_geom", row.getValue("origin_geom"))
                    .put("destination_geom", row.getValue("destination_geom"));
                    response.add(rowJson);
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put("id_commission", parameterBatch.get(0).get("id_commission"));
                    parameters.put("id_quotes", row.getValue("id_quotes"));
                 
                  // Create origin and destination geometry points
                     LOG.info("PARAMETERS ", parameters.toString());
                   // inserisco i valori in CACHE
                   insertValue(context, parameters);
                });

                    LOG.info("Path {} responds with {}", context.normalizedPath(), response.encode());
                    context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                    .end(response.encode());
                }).subscribe(succ->{},err -> {
                LOG.debug("Failure2: ", err , err.getMessage());});

      }

  private void insertValue(final RoutingContext context, Map<String, Object>  parameters) {
    SqlTemplate.forUpdate(db,
    "INSERT INTO schema.cache VALUES (#{id_commission},#{id_quotes})")// +"ON CONFLICT (id_quotes) DO NOTHING\") id_commission,id_quotes")//+ "ON CONFLICT (#{id_commission},#{id_quotes}) DO NOTHING")
    .rxExecute(parameters)
    .doOnError(err-> {LOG.debug("Failure: ", err , err.getMessage());})
    .doOnSuccess(result -> {
      
      LOG.info("Add quotes in chache: {}",result);
      //if(context.response().ended()){
      context.response()
      .setStatusCode(200)
      .end("Element added successfully");
     // }
    }).subscribe(succ->{},err -> {
                LOG.debug("Failure INSERT: ", err , err.getMessage());});
  }  
}
