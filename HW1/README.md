# BDMA_HW
This repository holds all the pieces of homework done at the "Big Data Mining and Applications" class at the National Taipei University of Technology in Fall 2019.

# Environment Setup
tbd...

# Framework Versions

 * Python 3.7.3
 * Spark  todo
 
# Report and ToDo's

The Reports and ToDo's are managed via Google-Drive:
https://docs.google.com/document/d/1N_UKlgGohHZXpln4wLSVmAgZYo6mHDzPKwiSeX_M7hg/edit?fbclid=IwAR1DC72F7gKzQ1nGgcpF2qZDg6h9zHiLrY6Rkinria1wcMLzUgwNXtHIiOo


Spark cluster setup:
1. cd docker-spark
2. docker-compose up

App setup:
docker build --rm -t hpc-app .
docker run -it --name hpc-app -e ENABLE_INIT_DAEMON=false --link spark-master:spark-master  --net docker-spark_default  -d hpc-app

