#!/bin/bash
$SPARK_HOME/bin/spark-submit --class "basics.SimpleJoin" --master "local" target/handson-spark-1.0.jar 
