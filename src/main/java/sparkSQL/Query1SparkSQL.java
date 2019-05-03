package sparkSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.List;

public class Query1SparkSQL {

    public static void process (JavaRDD<Tuple3<String, String, Double>> values) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query1").master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();


        Dataset<Row> df = createSchemaFromPreprocessedData(spark, values);


        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("query1");

        Dataset<Row> result = spark.sql("SELECT house_id, SUM(value) AS sum FROM query1  " +
                "GROUP BY house_id, timestamp");
        result.createOrReplaceTempView("temp");
        Dataset<Row> sqlDF = spark.sql("SELECT DISTINCT house_id FROM temp WHERE sum >= 350 ");


        sqlDF.show();
        spark.close();

    }

    private static Dataset<Row> createSchemaFromPreprocessedData(SparkSession spark,
                                                                 JavaRDD<Tuple3<String, String, Double>> values){

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("house_id",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("timestamp",     DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value",         DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = values.map(new Function<Tuple3<String, String,Double>, Row>() {
            @Override
            public Row call(Tuple3<String, String, Double> val) throws Exception {
                return RowFactory.create(val._1(), val._2(), val._3());
            }
        });

        // Apply the schema to the RDD
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        return df;

    }

}
