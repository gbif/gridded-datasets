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
mvn clean package
```

## How to run

```shell
# Copy gridded-datasets artifact to Gateway VH
scp spark-process/target/spark-process-1.0-SNAPSHOT.jar your_userk@c3gateway-vh.gbif.org:.

# dev - database name
# occurrence - table name
# public.dataset_gridded - output table
sudo -u hdfs spark2-submit --conf spark.executor.memoryOverhead=2048 --class org.gbif.gridded.datasets.GriddedDatasets --master yarn --executor-memory 8G --executor-cores 4 --num-executors 2 --driver-memory 1G spark-process-1.0-SNAPSHOT.jar --hive-db dev --hive-table-occurrence occurrence --jdbc-url jdbc:postgresql://pg1.gbif-dev.org/dev_registry --jdbc-user user --jdbc-password password --jdbc-table public.dataset_gridded
```
