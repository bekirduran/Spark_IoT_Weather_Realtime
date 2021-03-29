package com.bigdata.iotweatherapp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class Application {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession = SparkSession.builder().appName("Spark Steam Chat Listener")
                .master("local").getOrCreate();
        //Data schemas
        StructType weatherStruct = new StructType()
                .add("city","string")
                .add("heatSign","string")
                .add("heat","integer")
                .add("windWay","string")
                .add("wind","integer");
        // DataSource Path.
        Dataset<Row> rowDataset = sparkSession.readStream().schema(weatherStruct).option("sep", ",").csv("C:\\Users\\morph\\Desktop\\bigdata\\iot_weather_sensors\\*");
       // Show selected rows and query
        Dataset<Row> heatFilter = rowDataset.select("city", "heat").where("heat>20");
        // Writing real-time on console
        StreamingQuery start = heatFilter.writeStream().outputMode("update").format("console").start();

            start.awaitTermination();


    }
}
