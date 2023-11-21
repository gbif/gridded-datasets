# Gridded datasets

How to install:
Use airflow template located here: [gbif-gridded-datasets-spark](https://github.com/gbif/gbif-airflow-dags/blob/develop/dags/gbif-gridded-datasets-spark.py)

# Algorithm description 

This [blog post](https://data-blog.gbif.org/post/finding-gridded-datasets/) describes the basic algorithm for finding gridded datasets in more detail.

**gridded example**

![](https://raw.githubusercontent.com/jhnwllr/charts/master/griddedNN.gif)

**not gridded example**

![](https://raw.githubusercontent.com/jhnwllr/charts/master/notGriddedNN.gif)

Basically the algorithm searches for gridded datasets by computing one feature: **the percent of nearest neighbor distances (euclidean) that are the same**.

After this feature is computed the following datasets are classified as **gridded**: 

1. Datasets with >20 unique lat-lon points.
2. Datasets that have >30% of their unique lat-lon points with the same nearest neighbor distance.
3. Datasets with a nearest neighbor distances >0.02 decimal degrees.

This implementation is limited to datasets with <30,000 unique (lat,lon) points. It uses [local sensitivity hashing](https://en.wikipedia.org/wiki/Locality-sensitive_hashing) to reduce the NxN search space for nearest neighbor distances. Because of this sometimes datasets on a small grid (approx. < 0.1) with a large amount of unique points (>30,000), will not be found.   

Most gridded datasets on GBIF fill in 1 of the 3 following fields (usually with a constant value): 

1. coordinateUncertainyInMeters
2. coordinatePrecision
3. footPrintWKT

Since **not all gridded dataset publishers fill in these fields**, this project acts as a convenient way to identify them. 

# Output description

The end result of the gridded dataset search will produce a table with the following columns:

* **totalCount** (total_count) : the total number unique lat-lon points in the dataset. 
* **minDist** (min_dist) : the most common nearest neighbor (minimum) distance between unique lat-lon points.  
* **minDistCount** (min_dist_count) : the number of nearest neightbor distances that are equal to the **minDist**. 
* **maxPercent** (max_percent) : the percentage (fraction 0-1) of unique lat-lon points that have the same nearest neighbor distance. 

**maxPercent** is the main way to identify a "gridded" dataset. If this number is high (> 0.3), then the dataset is considered gridded. 


## Prepare Postgres table

```postgres-sql
CREATE TABLE IF NOT exists public.dataset_gridded (
	"key" bigserial NOT NULL,
	dataset_key uuid NOT NULL,
	total_count int,
    	min_dist float,
    	min_dist_count int,
    	"percent" float,
    	max_percent float,
	CONSTRAINT dataset_gridded_pk PRIMARY KEY (key)
);

CREATE INDEX IF NOT EXISTS dataset_griddeds_dataset_key_idx ON dataset_gridded(dataset_key);
```

## How to build

```shell
mvn clean install docker:build docker:push
```

## How to run

```shell
# Copy gridded-datasets artifact to Gateway VH
scp spark-process/target/spark-process-1.1.0-SNAPSHOT.jar your_userk@c3gateway-vh.gbif.org:.

# dev - database name
# occurrence - table name
# public.dataset_gridded - output table
sudo -u hdfs spark2-submit --conf spark.executor.memoryOverhead=2048 --class org.gbif.gridded.datasets.GriddedDatasets --master yarn --executor-memory 8G --executor-cores 4 --num-executors 2 --driver-memory 1G spark-process-1.1.0-SNAPSHOT.jar --hive-db dev --hive-table-occurrence occurrence --jdbc-url jdbc:postgresql://pg1.gbif-dev.org/dev_registry --jdbc-user user --jdbc-password password --jdbc-table public.dataset_gridded
```
