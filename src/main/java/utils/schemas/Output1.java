package utils.schemas;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Output1 {

    public static StructType getSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("year", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("citiesList",DataTypes.StringType, false));


         return DataTypes.createStructType(fields);

    }
}