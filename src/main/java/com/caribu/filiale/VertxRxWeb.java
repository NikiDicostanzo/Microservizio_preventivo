package com.caribu.filiale;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.http.HttpHeaders;
import io.vertx.rxjava3.core.http.HttpServer;
import io.vertx.rxjava3.ext.web.Router;
import io.vertx.rxjava3.ext.web.RoutingContext;
import io.vertx.rxjava3.ext.web.handler.BodyHandler;
import io.vertx.rxjava3.ext.web.openapi.RouterBuilder;
import io.vertx.rxjava3.pgclient.PgPool;
import io.vertx.rxjava3.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caribu.filiale.config.BrokerConfig;
import com.caribu.filiale.config.ConfigLoader;
import com.caribu.filiale.eliminare.GetAllOpFromDatabaseHandler;
import com.caribu.filiale.operatorInf.DeleteOpDatabaseHandler;
import com.caribu.filiale.operatorInf.GetOpFromDatabaseHandler;
import com.caribu.filiale.operatorInf.GetSameTratta;
import com.caribu.filiale.operatorInf.PostOpFromDatabaseHandler;
import com.caribu.filiale.operatorInf.PutOpDatabaseHandler;

public class VertxRxWeb extends AbstractVerticle {

  private static final Logger LOG = LoggerFactory.getLogger(VertxRxWeb.class);

  @Override
  public Completable rxStart() {
      return ConfigLoader.load(vertx)
      .doOnSuccess(configuration -> {
        LOG.info("Retrieved Configuration: {}", configuration);

        startHttpServerAndAttachRoutes(configuration);
      })
      .doOnError(configuration -> {
        LOG.info("Errore: {}", configuration);
      })
      .ignoreElement();
  }
  private void startHttpServerAndAttachRoutes(final BrokerConfig configuration){

    final var poolOptions = new PoolOptions()
      .setMaxSize(4);

    final var connectOptions = new PgConnectOptions()
      .setHost(configuration.getDbConfig().getHost())
      .setPort(configuration.getDbConfig().getPort())
      .setDatabase(configuration.getDbConfig().getDatabase())
      .setUser(configuration.getDbConfig().getUser())
      .setPassword(configuration.getDbConfig().getPassword());
    LOG.debug("DB Config: {}", connectOptions.getHost());

    final Pool db = PgPool.pool(vertx, connectOptions, poolOptions);

    RouterBuilder.create(vertx, "openapi.yml")
        .doOnSuccess(routerBuilder -> { // (1)

        //routerBuilder.operation("listQuotes").handler(new GetAllOpFromDatabaseHandler(db)); // (3)
        routerBuilder.operation("listQuotes").handler(new GetSameTratta(db)); // (3)
        routerBuilder.operation("getQuotes").handler(new GetOpFromDatabaseHandler(db)); // (3)
        //routerBuilder.operation("updateOpAvailability").handler(new PutOpDatabaseHandler(db)); // (3)
        routerBuilder.operation("deleteQuotes").handler(new DeleteOpDatabaseHandler(db)); // (3)
        routerBuilder.operation("addQuotes").handler(new PostOpFromDatabaseHandler(db)); // (3)
        
        Router restApi = routerBuilder.createRouter();
        restApi.route().handler(BodyHandler.create());
      
        restApi.route().handler(this::failureHandler);

        Single<HttpServer> single = vertx.createHttpServer()
          .requestHandler(restApi)
          .rxListen(8888,"localhost");
          single.subscribe(
            server -> {LOG.info("Server Start");
            },
            failure -> {LOG.error("Server could not start: (1) " + failure.getMessage(), failure);
            }
          );       
      }).doOnError(cause -> { 
            LOG.error("Server could not start: (2)" + cause.getMessage(), cause);
    }).subscribe();
  }

  private void failureHandler(RoutingContext errorContext) {
        
        if (errorContext.response().ended()) {
          // Ignore completed response
          LOG.info("------");
          return;
        }
        LOG.info("Route Error:", errorContext.failure());
        errorContext.response()
          .setStatusCode(500)
          .end(new JsonObject().put("message: Something went wrong, path: ", errorContext.normalizedPath()).toString());
    }
  public static String getAccountId(final RoutingContext context) {
        String id =  context.pathParam("id");//Integer.parseInt();
        LOG.debug("{} for account {}", context.normalizedPath(), id);
        return id;
  }

}




        //Router router = Router.router(vertx);
        //router.route().handler(BodyHandler.create());
        // router.get("/quotes").handler(new GetAllOpFromDatabaseHandler(db));//this::hello);//
        // router.get("/quotes/delete/:id").handler(new GetAllOpFromDatabaseHandler(db));//this::hello);//

