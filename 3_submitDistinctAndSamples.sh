#!/bin/bash
$SPARK_HOME/bin/spark-submit --class "basics.DistinctAndSample" --master "local" target/handson-spark-1.0.jar 
