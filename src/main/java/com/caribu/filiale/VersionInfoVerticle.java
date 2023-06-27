package com.caribu.filiale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caribu.filiale.config.ConfigLoader;

import io.reactivex.rxjava3.core.Completable;
import io.vertx.rxjava3.core.AbstractVerticle;



public class VersionInfoVerticle extends AbstractVerticle {

  private static final Logger LOG = LoggerFactory.getLogger(VersionInfoVerticle.class);

  @Override
  public Completable rxStart() {
      return ConfigLoader.load(vertx)
        .doOnSuccess(configuration -> LOG.info("CIAOOOOOOCurrent Application Version is: {}", configuration.getVersion()))
        .doOnError(throwable -> LOG.error("Failed to load configuration", throwable))
         .ignoreElement();
    }
}
