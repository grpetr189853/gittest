package com.epam.bigData;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;

import java.util.Properties;

import static com.epam.bigData.constant.Constants.DRIVER_MEMORY_KEY;
import static com.epam.bigData.constant.Constants.DRIVER_MEMORY_VALUE;
import static com.epam.bigData.constant.Constants.EXECUTOR_MEMORY_KEY;
import static com.epam.bigData.constant.Constants.EXECUTOR_MEMORY_VALUE;
import static com.epam.bigData.constant.Constants.SPARK_CORES_MAX_KEY;
import static com.epam.bigData.constant.Constants.SPARK_CORES_MAX_VALUE;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class ConversionConsumer {

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession session = SparkSession.builder()
                .master("local[8]")
                .config(EXECUTOR_MEMORY_KEY, EXECUTOR_MEMORY_VALUE)
                .config(DRIVER_MEMORY_KEY, DRIVER_MEMORY_VALUE)
                .config(SPARK_CORES_MAX_KEY, SPARK_CORES_MAX_VALUE)
                .config("spark.driver.maxResultSize","2G")
                .config("spark.executor.extraJavaOptions","XX:+UseG1GC")
                .config("spark.sql.shuffle.partitions",100)
                .config("spark.driver.maxResultSize", "4g")
                .config("spark.app.hw5.db.user","root")
                .config("spark.app.hw5.db.password","18985320061980")
                .config("spark.app.hw5.db.url","jdbc:mysql://localhost:3306/hw5")
                .config("spark.app.hw5.db.table","conversion")
                .config("spark.app.hw5.top.categories.amount","5")
                .appName("Streaming App")
                .getOrCreate();


        session.sparkContext().setLogLevel("ERROR");

        Dataset<Row> clickStream = getMessagesClickStreamTopic(session);

        Dataset<Row> buyStream = getMessagesBuyStreamTopic(session);

        Dataset<Row> outputDF = clickStream
                .join(buyStream,
                expr("clickSessionId = buySessionId AND clickTimestamp <= buyTimestamp AND buyTimestamp <= clickTimestamp + interval 12 hours"),"left_outer");
        Dataset<Row> averageConversion = outputDF
                .groupBy(col("category"), col("clickSessionId"), window(col("clickTimestamp"), "1 days","12 hours"))
                .agg(approx_count_distinct("buyItemId")
                        .divide(approx_count_distinct("clickItemId")).as("conversion"));

//        Long inputDfSize = outputDF.count();
//        clickStream.writeStream()
//                .format("console")
//                .outputMode("append")
//                .trigger(Trigger.ProcessingTime("10 minutes")).start();
//        buyStream.writeStream()
//                .format("console")
//                .outputMode("append")
//                .trigger(Trigger.ProcessingTime("10 minutes")).start();
//        Dataset<Row> clickStreamAgg = clickStream.groupBy(col("category"), window(col("clickTimestamp"), "1 seconds")).agg(count(col("category")));

        averageConversion.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (v1, v2) -> {
            Properties properties = new Properties();
            properties.setProperty("user", session.conf().get("spark.app.hw5.db.user"));
            properties.setProperty("password", session.conf().get("spark.app.hw5.db.password"));
            properties.put("driver", "com.mysql.cj.jdbc.Driver");
            String dbUrl = session.conf().get("spark.app.hw5.db.url");
            String tableName = session.conf().get("spark.app.hw5.db.table");

            v1.sort(col("conversion"))
                    .select(col("category"), col("conversion"))
                    .limit(Integer.parseInt(session.conf().get("spark.app.hw5.top.categories.amount")))
                    .write().mode(SaveMode.Append).jdbc(dbUrl, tableName, properties);
            })
            .outputMode("append")
            .trigger(Trigger.ProcessingTime("1 minutes"))
            .start()
            .awaitTermination();

        /*
        StreamingQuery streamingQuery = outputDF.writeStream()
                .format("console")
                .outputMode("append")
                .trigger(Trigger.ProcessingTime("1 minutes"))
                .start();
        streamingQuery.awaitTermination();
        */
    }

    public static Dataset<Row> getMessagesClickStreamTopic(SparkSession sparkSession) {
        Dataset<Row> clickStream = (sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "clickstream")
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger","100000")
                .load());

        DataType clicksSchema = new StructType()
                .add("sessionId", IntegerType, false, Metadata.empty())
                .add("eventTime", StringType, false, Metadata.empty())
                .add("itemId", IntegerType, false, Metadata.empty())
                .add("category", StringType, false, Metadata.empty());

        clickStream = clickStream.select(from_json(col("value").cast(StringType), clicksSchema).alias("value"))
                .select(col("value.sessionId").as("clickSessionId"),
                        to_timestamp(col("value.eventTime")).as("clickTimestamp"),
                        col("value.itemId").as("clickItemId"),
                        col("value.category").as("category")).withWatermark("clickTimestamp","1 hours");
        return clickStream;
    }

    public static Dataset<Row> getMessagesBuyStreamTopic(SparkSession sparkSession){
        Dataset<Row> buyStream = (sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "buystream")
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger","100000")
                .load());

        DataType buysSchema = new StructType()
                .add("sessionId", IntegerType, false, Metadata.empty())
                .add("eventTime", StringType, false, Metadata.empty())
                .add("itemId", IntegerType, false, Metadata.empty())
                .add("price", DoubleType, false, Metadata.empty())
                .add("quantity", IntegerType, false, Metadata.empty());

        buyStream = buyStream.select(from_json(col("value").cast(DataTypes.StringType), buysSchema).alias("value"))
                .select(col("value.sessionId").as("buySessionId"),
                        to_timestamp(col("value.eventTime")).as("buyTimestamp"),
                        col("value.itemId").as("buyItemId"),
                        col("value.price").as("price"),
                        col("value.quantity").as("quantity")
                ).withWatermark("buyTimestamp","1 hours");
        return buyStream;
    }
}
