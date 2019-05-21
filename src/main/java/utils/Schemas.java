package utils;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Schemas {

    public static StructType getSchema1() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("Index", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Year",DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("City",DataTypes.StringType, false));


        return DataTypes.createStructType(fields);

    }
    public static StructType getSchema2() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("Nation", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Year",DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Month",DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Mean",DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("Stddev",DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("Min",DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("Max",DataTypes.DoubleType, false));

        return DataTypes.createStructType(fields);

    }

    public static StructType getSchema3() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("City", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Nation",DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Position",DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("Year",DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Diffvalue",DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("Previousposition",DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("Previousyear",DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Previousdiffvalue",DataTypes.DoubleType, false));

        return DataTypes.createStructType(fields);

    }
}
