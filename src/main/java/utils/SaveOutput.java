package utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.Serializable;

public class SaveOutput implements Serializable {

    public void saveOutputQuery1(JavaRDD<Tuple3<String,String,String>> result, SparkSession sparkSession, String hdfs ){
        JavaRDD<Row> rows = result.map((Function<Tuple3<String, String, String>, Row>) stringStringStringTuple3 -> RowFactory.create(
                stringStringStringTuple3._1(),
                stringStringStringTuple3._2(),
                stringStringStringTuple3._3()
        ));

        Dataset<Row> df = sparkSession.sqlContext().createDataFrame(rows, Schemas.getSchema1());

        df.coalesce(1)
                .write()
                .format("csv")
                .option("header", "true")
                .save(hdfs + "/output1/output1.csv");


    }

    public void saveOutputQuery2(JavaPairRDD<Tuple3<String,String,String>, Tuple4<Double,Double,Double,Double>> result , SparkSession sparkSession, String hdfs,String path ){
        JavaRDD<Row> rows = result.map((Function<Tuple2<Tuple3<String, String, String>, Tuple4<Double, Double, Double, Double>>, Row>)
                tuple3Tuple4Tuple2 -> RowFactory.create(
                tuple3Tuple4Tuple2._1()._1(),
                tuple3Tuple4Tuple2._1()._2(),
                tuple3Tuple4Tuple2._1()._3(),
                tuple3Tuple4Tuple2._2()._1(),
                tuple3Tuple4Tuple2._2()._2(),
                tuple3Tuple4Tuple2._2()._3(),
                tuple3Tuple4Tuple2._2()._4()

                ));

        Dataset<Row> df = sparkSession.sqlContext().createDataFrame(rows, Schemas.getSchema2());

        df.coalesce(1)
                .write()
                .format("csv")
                .option("header", "true")
                .save(hdfs + path);


    }

    public void saveOutputQuery3(JavaPairRDD<Tuple2<String,String>, Tuple2<Tuple3<Integer,String,Double>,Tuple3<Integer,String,Double>>> result , SparkSession sparkSession, String hdfs ){
        JavaRDD<Row> rows = result.map((Function<Tuple2<Tuple2<String, String>, Tuple2<Tuple3<Integer, String, Double>, Tuple3<Integer, String, Double>>>, Row>)
                tuple2Tuple2Tuple2 -> RowFactory.create(
                        tuple2Tuple2Tuple2._1()._1(),
                        tuple2Tuple2Tuple2._1()._2(),
                        tuple2Tuple2Tuple2._2()._1()._1(),
                        tuple2Tuple2Tuple2._2()._1()._2(),
                        tuple2Tuple2Tuple2._2()._1()._3(),
                        tuple2Tuple2Tuple2._2()._2()._1(),
                        tuple2Tuple2Tuple2._2()._2()._2(),
                        tuple2Tuple2Tuple2._2()._2()._3()
                )

        );

        Dataset<Row> df = sparkSession.sqlContext().createDataFrame(rows, Schemas.getSchema3());

        df.coalesce(1)
                .write()
                .format("csv")
                .option("header", "true")
                .save(hdfs + "/output3/output3.csv");


    }

    public void saveTimes(JavaRDD<Tuple2<Long,Long>> result, SparkSession sparkSession, String hdfs, String filename ){
        JavaRDD<Row> rows=result.map((Function<Tuple2<Long, Long>, Row>) longLongTuple2 -> RowFactory.create(
                longLongTuple2._1(),
                longLongTuple2._2()
        ));

        Dataset<Row> df = sparkSession.sqlContext().createDataFrame(rows, Schemas.getSchemaTimes());

        df.coalesce(1)
                .write()
                .format("csv")
                .option("header", "true")
                .save(hdfs + "/times/"+filename);

    }
}
