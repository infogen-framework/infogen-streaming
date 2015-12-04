#!/bin/sh
mvn compile package -Dmaven.test.skip

spark-submit --class ETL --master yarn-cluster  --executor-memory 512m target/infogen_analysis.jar

