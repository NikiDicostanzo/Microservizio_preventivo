package com.caribu.filiale.config;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DbConfig {

  /* String host = "my-postgres";
  int port = 5433;
  String database = "vertx";
  String user="postgres";
  String password = "secret"; */

  String host;
  int port;
  String database;
  String user;
  String password;

  @Override
  public String toString() {
    return "DbConfig{" +
      "host='" + host + '\'' +
      ", port=" + port +
      ", database='" + database + '\'' +
      ", user='" + user + '\'' +
      ", password='******'" +
      '}';
  }
}