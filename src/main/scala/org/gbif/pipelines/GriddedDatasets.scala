package org.gbif.pipelines

import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.math.sqrt

object GriddedDatasets {

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

    // remove eBird, artportalen, observation.org, iNaturalist
    val excludeDatasets = Set(
      "4fa7b334-ce0d-4e88-aaae-2e0c138d049e",
      "38b4c89f-584c-41bb-bd8f-cd1def33e92f",
      "8a863029-f435-446a-821e-275f4f641165",
      "50c9509d-22c7-4a22-a47d-8c48425ef4a7"
    ).toSeq

    val spark = SparkSession.builder().appName("gridded_datasets").getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR") // reduce printed output

    //Firsts argument is the database
    val database = args(0)

    //Second argument is the table
    val table = args(1)

    //Output table
    val outTable = args(2)

    val occurrences = spark.sql("SELECT datasetkey, decimallatitude, decimallongitude FROM " + database + "." + table)
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
    val dfExportBig = project(dfVector, datasetCounts, 7000, 50000, 0.1)(spark)

    // export data to tsv
    Seq(dfExportSmall, dfExportBig)
      .reduce(_ union _)
      // Merge all columns into structure
      .withColumn("struct", struct("total_count", "min_dist", "min_dist_count", "percent", "max_percent"))
      // Drop all extra columns, leave dataset and struct only
      .drop("total_count")
      .drop("min_dist")
      .drop("min_dist_count")
      .drop("percent")
      .drop("max_percent")
      // Group by datasetkey key and convert to json, dataset_key -> json[]
      .groupBy(col("datasetkey").as("dataset_key")).agg(to_json(collect_list("struct")).as("json"))
      // Write to single csv file with headers
      .repartition(1)
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(outTable)
  }

}
