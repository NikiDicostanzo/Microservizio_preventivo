package com.caribu.filiale.eliminare;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caribu.filiale.db.DbResponse;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava3.ext.web.RoutingContext;
import io.vertx.rxjava3.sqlclient.Pool;


public class GetAllOpFromDatabaseHandler implements Handler<RoutingContext> {

  private static final Logger LOG = LoggerFactory.getLogger(GetAllOpFromDatabaseHandler.class);
  private final Pool db;

  public GetAllOpFromDatabaseHandler(final Pool db) {
    this.db = db;
  }
  @Override
  public void handle(final RoutingContext context) {
    LOG.info("Executing DB query to find all users...");
    db.query("SELECT * FROM schema.quotes a")
      .rxExecute()
      .doOnSuccess(result -> {
        LOG.info("Got " + result.size() + " rows ");
        JsonArray response = new JsonArray();
        result.forEach(row -> {
          JsonObject rowJson = new JsonObject()
            .put("id", row.getValue("id"))
            .put("origine", row.getValue("origine"))
            .put("destinazione", row.getValue("destinazione"))
            .put("km", row.getValue("km"));
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
      .subscribe(); // Don't forget to subscribe to the Single
  }

}