package com.google.spez.spanner;

import com.google.spez.core.SpezConfig;
import com.google.spez.spanner.internal.BothanDatabase;
import com.google.spez.spanner.internal.GaxDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseFactory {
  private static final Logger log = LoggerFactory.getLogger(DatabaseFactory.class);

  public static Database openDatabase(boolean useCustomClient, Settings settings, String databasePath) {
    log.info("Building database with path '{}'", databasePath);
    if (useCustomClient) {
      return BothanDatabase.openDatabase(settings);
    }
    return GaxDatabase.openDatabase(settings);
  }

  public static Database openLptsDatabase(SpezConfig.LptsConfig config) {
    return openDatabase(config.useCustomClient(), config.getSettings(), config.databasePath());
  }

  public static Database openSinkDatabase(SpezConfig.SinkConfig config) {
    return openDatabase(config.useCustomClient(), config.getSettings(), config.databasePath());
  }

}
