package org.gbif.gridded.datasets

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.common.base.Preconditions
import com.google.common.io.Resources


/**
 * Utility builders
 */
object Configurations {

  /**
   * Returns the application configuration for the given file.
   *
   * @param file The YAML config file on the classpath
   * @return The application configuration
   */
  def fromFile(file: String): GriddedConfiguration = {
    val confUrl = Resources.getResource(file)
    val mapper = new ObjectMapper(new YAMLFactory())
    val config: GriddedConfiguration = mapper.readValue(confUrl, classOf[GriddedConfiguration])
    config
  }
}

/**
 * The configuration for the backfill tile jobs.
 */
class GriddedConfiguration(
                            @JsonProperty("hive") _hive: HiveConfiguration,
                            @JsonProperty("registry") _registry: RegistryConfiguration
                          ) extends Serializable {
  val hive = Preconditions.checkNotNull(_hive, "hive cannot be null": Object)
  val registry = Preconditions.checkNotNull(_registry, "registry cannot be null": Object)
}


/**
 * Configuration specific to the Hive.
 */
class HiveConfiguration(
                         @JsonProperty("database") _database: String,
                         @JsonProperty("table") _table: String
                       ) extends Serializable {
  val database = Preconditions.checkNotNull(_database, "database cannot be null": Object)
  var table = Preconditions.checkNotNull(_table, "table cannot be null": Object)
}


/**
 * Configuration specific to the Registry.
 */
class RegistryConfiguration(
                             @JsonProperty("jdbc") _jdbc: String,
                             @JsonProperty("user") _user: String,
                             @JsonProperty("password") _password: String,
                             @JsonProperty("table") _table: String
                           ) extends Serializable {
  val jdbc = Preconditions.checkNotNull(_jdbc, "jdbc cannot be null": Object)
  var user = Preconditions.checkNotNull(_user, "user cannot be null": Object)
  var password = Preconditions.checkNotNull(_password, "password cannot be null": Object)
  var table = Preconditions.checkNotNull(_table, "table cannot be null": Object)
}
