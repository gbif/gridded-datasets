## How to build
```shell
mvn clean build
```

## Prepare Postgres table
```postgres-sql
CREATE TABLE public.dataset_gridded (
	"key" bigserial NOT NULL DEFAULT nextval('dataset_gridded_key_seq'::regclass),
	dataset_key uuid NOT NULL,
	grids json NULL,
	CONSTRAINT dataset_gridded_pk PRIMARY KEY (key),
	CONSTRAINT dataset_gridded_un UNIQUE (dataset_key),
	CONSTRAINT dataset_gridded_fk FOREIGN KEY (dataset_key) REFERENCES dataset(key)
);
```

## How to run

```shell
# Download JDBC driver and copy to Gateway VH
# Copy gridded-datasets artifact to Gateway VH
scp gridded-datasets/target/gridded-datasets-1.0-SNAPSHOT.jar your_userk@c3gateway-vh.gbif.org:.

# dev - database name
# occurrence - table name
# public.dataset_gridded - output table
sudo -u hdfs spark2-submit --conf spark.executor.memoryOverhead=2048 --class org.gbif.gridded.datasets.GriddedDatasets --master yarn --executor-memory 8G --executor-cores 4 --num-executors 2 --driver-memory 1G gridded-datasets-1.0-SNAPSHOT.jar --hive-db dev --hive-table-occurrence occurrence --jdbc-url jdbc:postgresql://pg1.gbif-dev.org/dev_registry --jdbc-user user --jdbc-password password --jdbc-table public.dataset_gridded
```
