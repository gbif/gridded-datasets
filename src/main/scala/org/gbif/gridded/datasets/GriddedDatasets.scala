package org.gbif.gridded.datasets

import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import scala.annotation.tailrec
import scala.math.sqrt

object GriddedDatasets {

  val usage = "Usage: configFile"

  def project(dfVector: DataFrame, datasetCounts: DataFrame, minRecordCount: Long, maxRecordCount: Long, bucketLength: Double)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val euclideanDistance = udf { (v1: Vector, v2: Vector) =>
      sqrt(Vectors.sqdist(v1, v2))
    }

    val dfFiltered = dfVector.join(datasetCounts, "datasetkey")
      .filter($"total_count" >= minRecordCount)
      .filter($"total_count" <= maxRecordCount)

    val vecToSeq = udf((v: Vector) => v.toArray).asNondeterministic

    val dfBucket = new BucketedRandomProjectionLSH()
      .setBucketLength(bucketLength)
      .setNumHashTables(1)
      .setInputCol("coord")
      .setOutputCol("hashes")
      .fit(dfFiltered)
      .transform(dfFiltered)
      .select($"datasetkey", $"coord", vecToSeq($"hashes"(0))(0) as "bucket")
      .withColumn("datasetkey_bucket", concat($"datasetkey", lit("_"), $"bucket"))

    val dfDist = dfBucket
      .groupBy("datasetkey_bucket")
      .agg(collect_list($"coord").as("lat_lon_list"))
      .join(dfBucket, "datasetkey_bucket")
      .select($"*", explode($"lat_lon_list").as("lat_lon"))
      .withColumn("dist", euclideanDistance($"coord", $"lat_lon"))
      .withColumn("rounded_dist", round(col("dist"), 2))
      .filter($"rounded_dist" > 0.01)
      .groupBy("datasetkey", "coord")
      .agg(min($"rounded_dist").as("min_dist")) // the nearest neighbor
      .groupBy("datasetkey", "min_dist")
      .agg(count(lit(1)).alias("min_dist_count"))

    val window = Window.partitionBy("datasetkey")

    datasetCounts.join(dfDist, "datasetkey")
      .withColumn("percent", round($"min_dist_count" / $"total_count", 4))
      .withColumn("max_percent", max(col("percent")).over(window))
      .filter($"percent" === $"max_percent")
      .cache()
  }

  def main(args: Array[String]) {

    checkArgs(args) // sanitize input

    // load application config
    val config: GriddedConfiguration = Configurations.fromFile(args(0))

    val hiveDatabase = config.hive.database
    val hiveTableOccurrence = config.hive.table
    val jdbcUrl = config.registry.jdbc
    val jdbcUser = config.registry.user
    val jdbcPassword = config.registry.password
    val jdbcTable = config.registry.table

    // remove eBird, artportalen, observation.org, iNaturalist
    val excludeDatasets = Set(
      "4fa7b334-ce0d-4e88-aaae-2e0c138d049e",
      "38b4c89f-584c-41bb-bd8f-cd1def33e92f",
      "8a863029-f435-446a-821e-275f4f641165",
      "50c9509d-22c7-4a22-a47d-8c48425ef4a7"
    ).toSeq

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Gridded datasets")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use " + hiveDatabase)

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR") // reduce printed output

    val occurrences = spark.sql("SELECT datasetkey, decimallatitude, decimallongitude FROM " + hiveTableOccurrence)
      .filter($"decimallatitude".isNotNull)
      .filter($"decimallongitude".isNotNull)
      .filter(!$"datasetkey".isin(excludeDatasets: _*))
      .withColumn("rounded_decimallatitude", round(col("decimallatitude"), 4))
      .withColumn("rounded_decimallongitude", round(col("decimallongitude"), 4))
      .select("datasetkey", "rounded_decimallatitude", "rounded_decimallongitude")
      .distinct()

    val dfVector = new VectorAssembler()
      .setInputCols(Array("rounded_decimallatitude", "rounded_decimallongitude"))
      .setOutputCol("coord")
      .transform(occurrences)
      .select("datasetkey", "coord")

    val datasetCounts = dfVector
      .groupBy("datasetkey")
      .agg(count(lit(1)).alias("total_count"))

    // split into big and small groups for bucket tuning
    val dfExportSmall = project(dfVector, datasetCounts, 20, 7000, 2)(spark)
    val dfExportBig = project(dfVector, datasetCounts, 7000, 30000, 0.1)(spark)

    // export data to db
    Seq(dfExportSmall, dfExportBig)
      .reduce(_ union _)
      // Rename datasetKey to dataset_key
      .withColumn("dataset_key", col("datasetkey"))
      .drop("datasetkey")
      // Write to DB table
      .repartition(1)
      .write
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcUrl)
      .option("dbtable", jdbcTable)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("truncate", true)
      .option("stringtype", "unspecified")
      .mode(SaveMode.Overwrite)
      .save()
  }

  /**
   * Sanitizes application arguments.
   */
  private def checkArgs(args: Array[String]) = {
    assert(args != null && args.length == 1, usage)
  }

}
