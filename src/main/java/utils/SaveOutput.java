package utils;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import utils.schemas.Output1;

import java.io.Serializable;

public class SaveOutput implements Serializable {

    public void saveOutputQuery1(JavaRDD<Tuple3<String,String,String>> result, SparkSession sparkSession, String hdfs ){
        JavaRDD<Row> rows = result.map(new Function<Tuple3<String, String, String>, Row>() {
            @Override
            public Row call(Tuple3<String, String, String> stringStringStringTuple3) throws Exception {
                return RowFactory.create(
                        stringStringStringTuple3._1(),
                        stringStringStringTuple3._2(),
                        stringStringStringTuple3._3()
                );
            }
        });

        Dataset<Row> df = sparkSession.sqlContext().createDataFrame(rows, Output1.getSchema());

        df.coalesce(1)
                .write()
                .format("csv")
                .option("header", "true")
                .save(hdfs + "/output1.csv");


    }
}
