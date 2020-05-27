package com.example.sparkdemo;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.upper;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

@Service
public class DemoApp {
    private StructType getSchema() {
        StructType phone = new StructType()
                .add("number", DataTypes.StringType)
                .add("type", DataTypes.StringType);

        ArrayType phones = new ArrayType(phone, true);

        StructType member = new StructType()
                .add("id", DataTypes.StringType)
                .add("name", DataTypes.StringType)
                .add("address", DataTypes.StringType)
                .add("phones", phones);

        return member;
    }

    public void start() {
        SparkSession spark = SparkSession.builder()
                .master("local[1]")
                .appName(DemoApp.class.getName())
                .getOrCreate();

        Dataset<Row> dataset = spark.read().format("json").schema(getSchema()).load("src/main/resources/member.json");
        Dataset<Row> d1 = dataset
                .select(col("*"), explode(col("phones")))
                .select(col("*"),
                        memberPhone("HOME").as("home_phone"),
                        memberPhone("WORK").as("work_phone"),
                        memberPhone("CELL").as("cell_phone")
                )
                .groupBy(col("id"), col("name"))
                .agg(
                        concat_ws("", collect_list("home_phone")).as("home_phone"),
                        concat_ws("", collect_list("work_phone")).as("work_phone"),
                        concat_ws("", collect_list("cell_phone")).as("cell_phone")
                );

        d1.show();
    }

    private Column memberPhone(final String phoneType) {
        return when(
                upper(col("col.type")).equalTo(phoneType.toUpperCase()),
                col("col.number")
        );
    }
}
