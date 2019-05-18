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
import utils.schemas.Output1;

import java.io.Serializable;

public class SaveOutput {

    public void saveOutputQuery1(JavaPairRDD<String, Iterable<String>> result,  SparkSession sparkSession, String hdfs ){
        JavaRDD<Row> rows = result.map(new Function<Tuple2<String, Iterable<String>>, Row>() {
            @Override
            public Row call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {

                return RowFactory.create(
                        stringIterableTuple2._1(),
                        Lists.newArrayList(stringIterableTuple2._2()).toString()
                );
            }
        });

        Dataset<Row> df = sparkSession.sqlContext().createDataFrame(rows, Output1.getSchema());

        df.coalesce(1)
                .write()
                .format("csv")
                .option("header", "true")
                .save(hdfs +"/output/output_1.csv");


    }
}
