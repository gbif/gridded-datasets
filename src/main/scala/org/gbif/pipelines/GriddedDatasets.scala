package org.gbif.pipelines

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector

import math.sqrt
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object GriddedDatasets {

  def project(occurrences: DataFrame, datasetCounts: DataFrame, minRecordCount: Long, maxRecordCount: Long, bucketLength: Double)(implicit sparkSession: SparkSession)  : DataFrame = {
    import sparkSession.implicits._
    val euclideanDistance = udf { (v1: Vector, v2: Vector) =>
      sqrt(Vectors.sqdist(v1, v2))
    }

    val dfFiltered = occurrences.join(datasetCounts, "datasetkey")
    .filter($"total_count" >= minRecordCount)
    .filter($"total_count" <= maxRecordCount)

    val brp = new BucketedRandomProjectionLSH()
    .setBucketLength(bucketLength)
    .setNumHashTables(1)
    .setInputCol("coord")
    .setOutputCol("hashes")

    val model = brp.fit(dfFiltered)


    val res = model.transform(dfFiltered)


    val vecToSeq = udf((v: Vector) => v.toArray).asNondeterministic

    val dfBucket = res.select($"datasetkey", $"coord", vecToSeq($"hashes"(0))(0) as "bucket")
      .withColumn("datasetkey_bucket",concat($"datasetkey", lit("_"), $"bucket"))


    val dfListColumn = dfBucket
      .groupBy("datasetkey_bucket")
      .agg(collect_list($"coord").as("lat_lon_list"))

    val dfJoined = dfListColumn.join(dfBucket,"datasetkey_bucket")


    val dfDist = dfJoined.select($"*", explode($"lat_lon_list").as("lat_lon"))
      .withColumn("dist", euclideanDistance($"coord", $"lat_lon"))
      .withColumn("rounded_dist", round(col("dist"),2))
      .filter($"rounded_dist" > 0.01)
      .groupBy("datasetkey","coord")
      .agg(min($"rounded_dist").as("min_dist")) // the nearest neighbor
      .groupBy("datasetkey","min_dist")
      .agg(count(lit(1)).alias("min_dist_count"))


    val df = datasetCounts.join(dfDist,"datasetkey")
      .withColumn("percent",round($"min_dist_count"/$"total_count",4))

    val windowSpec = Window.partitionBy("datasetkey")

    df.withColumn("max_percent", max(col("percent"))
      .over(windowSpec))
      .filter($"percent" === $"max_percent").
      cache()
  }

  def main(args: Array[String])
  {

    // remove eBird, artportalen, observation.org, iNaturalist
    val excludeDatasets = Set("4fa7b334-ce0d-4e88-aaae-2e0c138d049e", "38b4c89f-584c-41bb-bd8f-cd1def33e92f", "8a863029-f435-446a-821e-275f4f641165", "50c9509d-22c7-4a22-a47d-8c48425ef4a7")

    val spark = SparkSession.builder().appName("gridded_datasets").getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR") // reduce printed output

    //Firsts argument is the database
    val database = args(0)

    //Second argument is the table
    val table = args(1)

    //Second argument is the table
    val outTable = args(2)

    val occurrences = spark.sql("SELECT * FROM " + database + "." + table)
      .filter($"decimallatitude".isNotNull)
      .filter($"decimallongitude".isNotNull)
      .filter(!$"datasetkey".isInCollection(excludeDatasets))
      .withColumn("rounded_decimallatitude", round(col("decimallatitude"),4))
      .withColumn("rounded_decimallongitude", round(col("decimallongitude"),4))
      .select("datasetkey","rounded_decimallatitude","rounded_decimallongitude")
      .distinct()


    val assembler = new VectorAssembler()
      .setInputCols(Array("rounded_decimallatitude","rounded_decimallongitude"))
      .setOutputCol("coord")

    val datasetCounts = assembler.transform(occurrences)
      .select("datasetkey", "coord")
      .groupBy("datasetkey")
      .agg(count(lit(1)).alias("total_count"))

    // split into big and small groups for bucket tuning

    val dfExportSmall = project(occurrences, datasetCounts, 20, 7000, 2)

    val dfExportBig = project(occurrences, datasetCounts, 7000, 50000, 0.1)

    val dfExport = Seq(dfExportSmall, dfExportBig).reduce(_ union _)

    // export data to tsv


    dfExport
      .write.format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .mode(SaveMode.Overwrite)
      .save(outTable)

  }
}
