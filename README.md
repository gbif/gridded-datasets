## How to build
```shell
mvn clean build
```

## How to run

```shell
# Copy to Gateway VH
scp gridded-datasets/target/gridded-datasets-1.0-SNAPSHOT.jar your_userk@c3gateway-vh.gbif.org:.

# dev - database name
# occurrence - table name
# /tmp/gr/5 - output directory
sudo -u hdfs spark2-submit --conf spark.executor.memoryOverhead=2048 --class org.gbif.pipelines.GriddedDatasets --master yarn --executor-memory 8G --executor-cores 4 --num-executors 2 --driver-memory 1G gridded-datasets-1.0-SNAPSHOT.jar dev occurrence /tmp/gr/5
```
