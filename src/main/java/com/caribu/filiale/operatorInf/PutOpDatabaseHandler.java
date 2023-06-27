package com.caribu.filiale.operatorInf;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caribu.filiale.quotes;
import com.caribu.filiale.db.DbResponse;
import com.caribu.filiale.eliminare.WatchList;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;

public class PutOpDatabaseHandler implements Handler<RoutingContext>{

  private static final Logger LOG = LoggerFactory.getLogger(PutOpDatabaseHandler.class);
  private final Pool db;

  public PutOpDatabaseHandler(final Pool db) {
    this.db = db;
  }

  @Override
  public void handle(final RoutingContext context) {
    Integer id = OpRestApi.getAccountId(context);

    var json = context.body().asJsonObject();
    var watchList = json.mapTo(WatchList.class);

    var parameterBatch = watchList.getOp().stream()
      .map(op -> {
        final Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", id);
        parameters.put("isavailable", op.getIsavailable());
        return parameters;
      }).collect(Collectors.toList());
      LOG.info("ID",parameterBatch);
      SqlTemplate.forUpdate(db,"UPDATE broker.operator SET isavailable=(#{isavailable}) WHERE id=#{id}")
      .executeBatch(parameterBatch)
      .onFailure(DbResponse.errorHandler(context, "Failed to update into watchlist"))
      .onSuccess(result -> {
        LOG.info("OK");
        if(context.response().ended()){
        context.response()
        .setStatusCode(HttpResponseStatus.NO_CONTENT.code())
        .end();
        }
      });
    } 
    
}
