# Gridded datasets

How to install:

```
mvn clean install

cd oozie-workflow

./install-workflow.sh dev GITHUB_KEY
```
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

This implementation is limited to datasets with <50,000 unique (lat,lon) points. It uses [local sensitivity hashing](https://en.wikipedia.org/wiki/Locality-sensitive_hashing) to reduce the NxN search space for nearest neighbor distances. Because of this sometimes datasets on a small grid (approx. < 0.1) with a large amount of unique points (>50,000), will not be found.   

Most gridded datasets on GBIF fill in 1 of the 3 following fields (usually with a constant value): 

1. coordinateUncertainyInMeters
2. coordinatePrecision
3. footPrintWKT

Since **not all gridded dataset publishers fill in these fields**, this project acts as a convenient way to identify them. 

# Output description

The end result of the gridded dataset search will produce a table with the following columns:

**totalCount** (total_count) : the total number unique lat-lon points in the dataset. 
**minDist** (min_dist) : the most common nearest neighbor (minimum) distance between unique lat-lon points.  
**minDistCount** (min_dist_count) : the number of nearest neightbor distances that are equal to the **minDist**. 
**maxPercent** (max_percent) : the percentage (fraction 0-1) of unique lat-lon points that have the same nearest neighbor distance. 

**maxPercent** is the main way to identify a "gridded" dataset. If this number is high (> 0.3), then the dataset is considered gridded. 


