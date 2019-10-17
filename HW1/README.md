# BDMA_HW
This repository holds all the pieces of homework done at the "Big Data Mining and Applications" class at the National Taipei University of Technology in Fall 2019.

# Framework Versions

 * Python 3.7.4
 * Spark  2.4.4
 
# Report and ToDo's

The Reports and ToDo's are managed via Google-Drive:
https://docs.google.com/document/d/1N_UKlgGohHZXpln4wLSVmAgZYo6mHDzPKwiSeX_M7hg/edit?fbclid=IwAR1DC72F7gKzQ1nGgcpF2qZDg6h9zHiLrY6Rkinria1wcMLzUgwNXtHIiOo

# Setup

Spark cluster setup:
1. cd docker-spark
2. docker-compose up

App setup:
1. docker build --rm -t hpc-app .
2. docker run -it --name hpc-app -e ENABLE_INIT_DAEMON=false --link spark-master:spark-master  --net docker_spark_hadoop_default  -d hpc-app
:w

# RELATED:
https://github.com/big-data-europe/docker-spark/pull/87 

# Data Preprocessing

The following Data Preprocessing Tasks are done using Spark script:

* Fixing of Missing Data (indicated with ? instead of e.g. NaN)
* Fixing of Datatypes of columns of interest
* Dimension Reduction

# Results

The calculated results are stored in the /results directory.
 