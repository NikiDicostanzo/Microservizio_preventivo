package com.caribu.filiale.db.migration;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caribu.filiale.config.DbConfig;

import io.reactivex.rxjava3.core.Completable;
import io.vertx.rxjava3.core.Vertx;

public class FlywayMigration {

  private static final Logger LOG = LoggerFactory.getLogger(FlywayMigration.class);

  private FlywayMigration() {}

   public static Completable migrate(final Vertx vertx, final DbConfig dbConfig) {
    return vertx.<Void>executeBlocking(promise -> {
        execute(dbConfig);
        promise.complete();
    }).doOnError(err -> LOG.error("Failed to migrate db schema with error: ", err))
    .ignoreElement(); // Convert Single<Void> to Completable
  }


  private static void execute(final DbConfig dbConfig) {
    var database = "postgresql";
    //var database = "mysql";
    final String jdbcUrl = String.format("jdbc:%s://%s:%d/%s",
      database,
      dbConfig.getHost(),
      dbConfig.getPort(),
      dbConfig.getDatabase()
    );
    LOG.debug("Migrating DB schema using jdbc url: {}", jdbcUrl);

    final Flyway flyway = Flyway.configure()
      .dataSource(jdbcUrl, dbConfig.getUser(), dbConfig.getPassword())
      .schemas("schema")
      .defaultSchema("schema")
      .load();

    var current = Optional.ofNullable(flyway.info().current());
    current.ifPresent(info -> LOG.info("db schema is at version: {}", info.getVersion()));

    var pendingMigrations = flyway.info().pending();
    LOG.debug("Pending migrations are: {}", printMigrations(pendingMigrations));

    flyway.migrate();
  }

  private static String printMigrations(final MigrationInfo[] pending) {
    if (Objects.isNull(pending)) {
      return "[]";
    }
    return Arrays.stream(pending)
      .map(each -> each.getVersion() + " - " + each.getDescription())
      .collect(Collectors.joining(",", "[", "]"));
  }

}
