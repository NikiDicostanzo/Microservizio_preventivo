package com.caribu.filiale.operatorInf;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caribu.filiale.VertxRxWeb;
import com.caribu.filiale.quotes;
import com.caribu.filiale.db.DbResponse;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava3.ext.web.RoutingContext;
import io.vertx.rxjava3.sqlclient.Pool;
import io.vertx.rxjava3.sqlclient.templates.SqlTemplate;

public class GetOpFromDatabaseHandler implements Handler<RoutingContext> {

  private static final Logger LOG = LoggerFactory.getLogger(GetOpFromDatabaseHandler.class);
  private final Pool db;

  public GetOpFromDatabaseHandler(final Pool db) {
    this.db = db;
  }

  @Override
  public void handle(final RoutingContext context) {
    //final String nameParm = context.pathParam("nameop");
    final String id_tratta = context.pathParam("id_tratta");
    LOG.debug("Id parameter: {}", id_tratta);
//    final String id_tratta = context.pathParam("id_tratta");

    SqlTemplate.forQuery(db,
      "SELECT * from schema.tratta o where id_tratta=#{id_tratta}")
      .rxExecute(Collections.singletonMap("id_tratta", id_tratta))
      .doOnError(err -> {
          LOG.debug("Failure: ", err , err.getMessage());
          context.response()
            .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
            .setStatusCode(500)
            .end(new JsonObject().put("error", err.getMessage()).encode());
    })
      .doOnSuccess(op -> {
        if (!op.iterator().hasNext()) {
          DbResponse.notFound(context, "operator " + id_tratta + " not available!");
          return;
        }
        var response = op.iterator().next().toJson(); // operator class
        LOG.info("Path {} responds with {}", context.normalizedPath(), response.encode());
        context.response()
          .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
          .end(response.encode());
      }).subscribe();
  }
}
