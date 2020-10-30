package org.gbif.pipelines

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import math.sqrt

import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GriddedDatasets {
  def main(args: Array[String])
  {

    // remove eBird, artportalen, observation.org, iNaturalist
    val excludeDatasets = Set("4fa7b334-ce0d-4e88-aaae-2e0c138d049e", "38b4c89f-584c-41bb-bd8f-cd1def33e92f", "8a863029-f435-446a-821e-275f4f641165", "50c9509d-22c7-4a22-a47d-8c48425ef4a7")

    val spark = SparkSession.builder().appName("gridded_datasets").getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR") // reduce printed output

    val euclideanDistance = udf { (v1: Vector, v2: Vector) =>
      sqrt(Vectors.sqdist(v1, v2))
    }

    //Firsts argument is the database
    val database = args(0)

    //Second argument is the table
    val table = args(1)

    val df_original = spark.sql("SELECT * FROM " + database + "." + table)
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

    val df_vector = assembler.transform(df_original).select("datasetkey", "coord")

    val dataset_counts = df_vector
      .groupBy("datasetkey")
      .agg(count(lit(1)).alias("total_count"))

    // split into big and small groups for bucket tuning

    val upper_limit = 50000

    val df_filtered_1 = df_vector.join(dataset_counts,"datasetkey")
      .filter($"total_count" >= 20)
      .filter($"total_count" <= 7000)

    val df_filtered_2 = df_vector.join(dataset_counts,"datasetkey")
      .filter($"total_count" >= 7000)
      .filter($"total_count" <= upper_limit)

    val brp_1 = new BucketedRandomProjectionLSH()
      .setBucketLength(2)
      .setNumHashTables(1)
      .setInputCol("coord")
      .setOutputCol("hashes")

    val brp_2 = new BucketedRandomProjectionLSH()
      .setBucketLength(0.01)
      .setNumHashTables(1)
      .setInputCol("coord")
      .setOutputCol("hashes")

    val model_1 = brp_1.fit(df_filtered_1)
    val model_2 = brp_2.fit(df_filtered_2)

    val res_1 = model_1.transform(df_filtered_1)
    val res_2 = model_2.transform(df_filtered_2)

    val vecToSeq = udf((v: Vector) => v.toArray).asNondeterministic

    val df_bucket_1 = res_1.select($"datasetkey",$"coord", vecToSeq($"hashes"(0))(0) as "bucket")
      .withColumn("datasetkey_bucket",concat($"datasetkey",lit("_"),$"bucket"))
    val df_bucket_2 = res_2.select($"datasetkey",$"coord", vecToSeq($"hashes"(0))(0) as "bucket")
      .withColumn("datasetkey_bucket",concat($"datasetkey",lit("_"),$"bucket"))

    val df_list_column_1 = df_bucket_1
      .groupBy("datasetkey_bucket")
      .agg(collect_list($"coord").as("lat_lon_list"))
    val df_list_column_2 = df_bucket_2
      .groupBy("datasetkey_bucket")
      .agg(collect_list($"coord").as("lat_lon_list"))

    val df_joined_1 = df_list_column_1.join(df_bucket_1,"datasetkey_bucket")
    val df_joined_2 = df_list_column_2.join(df_bucket_2,"datasetkey_bucket")

    val df_dist_1 = df_joined_1.select($"*", explode($"lat_lon_list").as("lat_lon"))
      .withColumn("dist", euclideanDistance($"coord", $"lat_lon"))
      .withColumn("rounded_dist", round(col("dist"),2))
      .filter($"rounded_dist" > 0.01)
      .groupBy("datasetkey","coord")
      .agg(min($"rounded_dist").as("min_dist")) // the nearest neighbor
      .groupBy("datasetkey","min_dist")
      .agg(count(lit(1)).alias("min_dist_count"))

    val df_dist_2 = df_joined_2.select($"*", explode($"lat_lon_list").as("lat_lon"))
      .withColumn("dist", euclideanDistance($"coord", $"lat_lon"))
      .withColumn("rounded_dist", round(col("dist"),2))
      .filter($"rounded_dist" > 0.01)
      .groupBy("datasetkey","coord")
      .agg(min($"rounded_dist").as("min_dist")) // the nearest neighbor
      .groupBy("datasetkey","min_dist")
      .agg(count(lit(1)).alias("min_dist_count"))

    val df_1 = dataset_counts.join(df_dist_1,"datasetkey")
      .withColumn("percent",round($"min_dist_count"/$"total_count",4))
    val df_2 = dataset_counts.join(df_dist_2,"datasetkey")
      .withColumn("percent",round($"min_dist_count"/$"total_count",4))

    import org.apache.spark.sql.expressions.Window

    val windowSpec = Window.partitionBy("datasetkey")

    val df_export_1 = df_1.withColumn("max_percent", max(col("percent"))
      .over(windowSpec))
      .filter($"percent" === $"max_percent").
      cache()

    val df_export_2 = df_2.withColumn("max_percent", max(col("percent"))
      .over(windowSpec))
      .filter($"percent" === $"max_percent").
      cache()

    val df_export = Seq(df_export_1,df_export_2).reduce(_ union _)

    // export data to tsv

    import org.apache.spark.sql.SaveMode

    val save_table_name = "gridded_datasets"

    df_export
      .write.format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .mode(SaveMode.Overwrite)
      .save(save_table_name)

  }
}
