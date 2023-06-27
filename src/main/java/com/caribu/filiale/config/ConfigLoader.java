// Docker database
package com.caribu.filiale.config;

import java.util.Arrays;
import java.util.List;

import javax.sound.sampled.AudioFileFormat.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.rxjava3.core.Single;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava3.config.ConfigRetriever;
import io.vertx.rxjava3.core.Vertx;

public class ConfigLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);
  public static final String CONFIG_FILE = "application.yml";
  // Exposed Environment Variables
  public static final String SERVER_PORT = "SERVER_PORT";
  public static final String DB_HOST = "DB_HOST";
  public static final String DB_PORT = "DB_PORT";
  public static final String DB_DATABASE = "DB_DATABASE";
  public static final String DB_USER = "DB_USER";
  public static final String DB_PASSWORD = "DB_PASSWORD";
  static final List<String> EXPOSED_ENVIRONMENT_VARIABLES = Arrays.asList(SERVER_PORT,
    DB_HOST, DB_PORT, DB_DATABASE, DB_USER, DB_PASSWORD);

  public static Single<BrokerConfig> load(Vertx vertx) {
    final var exposedKeys = new JsonArray();
    EXPOSED_ENVIRONMENT_VARIABLES.forEach(exposedKeys::add);
    LOG.debug("Fetch configuration for {}", exposedKeys.encode());

    var envStore = new ConfigStoreOptions()
      .setType("env")
      .setConfig(new JsonObject().put("keys", exposedKeys));

    var propertyStore = new ConfigStoreOptions()
      .setType("sys")
      .setConfig(new JsonObject().put("cache", false));

    var yamlStore = new ConfigStoreOptions()
      .setType("file")
      .setFormat("yaml")
      .setConfig(new JsonObject().put("path", CONFIG_FILE));

    var retriever = ConfigRetriever.create(vertx,
      new ConfigRetrieverOptions()
        // Order defines overload rules
        .addStore(yamlStore)
        .addStore(propertyStore)
        .addStore(envStore)
    );
    return retriever.getConfig().map(BrokerConfig::from); 
  }
}
