package com.moviebooking.streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._

import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.internal.util.TableDef.Column

object SparkStructuredStreaming {

  var currentTime = System.currentTimeMillis()

  def processStream(kafkaBootstrapServers: String, kafkaTopic: String) = {
    val sparkConf = new SparkConf().setAppName("Spark")
      .set("spark.sql.files.ignoreMissingFiles", "true").setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .appName("StructuredKafkaProcessing")
      .getOrCreate()

    import spark.implicits._

    val csv = Source.fromFile("/Users/sagarbb/work/my_experiments/sparkstreaming/src/main/resources/customers.csv").mkString

    val broadcastWrapper = BroadcastWrapper(spark.sparkContext, csv)

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .load()
      .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")

//    val forceRefreshDf: DataFrame = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
//      .option("subscribe", "forceRefresh")
//      .load()
//      .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")


    val filererdKafkaDf = df.filter(row => {
      !row.getAs[String]("value").equals("true") }
    ).toDF()

    val cartDf = filererdKafkaDf.map(row => {
      val value = row.getAs[String]("value").split(",")
      Cart(value.head, value(1))
    }).toDF()


    val forceRefreshDf = df.filter(row => {
      row.getAs[String]("value").equals("true")}
    ).toDF()


    import org.apache.spark.sql.types._

    val schema = StructType(
      List(StructField("customerId", StringType, false), StructField("customerName", StringType, true)))

    val csvDf: DataFrame = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("/Users/sagarbb/work/my_experiments/sparkstreaming/src/main/resources/customers.csv")
      .toDF()

//    forceRefreshDf.writeStream.foreach(new ForeachWriter[Row] {
//      override def open(partitionId: Long, version: Long): Boolean = true
//
//      override def process(value: Row): Unit = {
//        println("row is " + value.getAs[Boolean]("value"))
//        broadcastWrapper.update("New String", true)
//      }
//
//      override def close(errorOrNull: Throwable): Unit = {}
//    }).start()


//

//    forceRefreshDf.writeStream.format("console")
//      .outputMode(OutputMode.Append()).trigger(Trigger.ProcessingTime(1))
//      .start().awaitTermination()

//    val joinedDf = cartDf.join(br, Seq("customerId"), "left")
//      .toDF()
//
    cartDf.writeStream.format("console").foreach(new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Row): Unit = {
        println(s"initial spark context is ${spark.sparkContext}")
        val newSparkContext = SparkContext.getOrCreate()
        println(s"new spark context is ${newSparkContext}")

        println(s"cart is ${value} and broadcast is ${broadcastWrapper.value(newSparkContext)}")
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }).start().awaitTermination()


//    joinedDf.writeStream.format("console")
//      .outputMode(OutputMode.Update())
//      .trigger(Trigger.ProcessingTime(1000)).start().awaitTermination()

  }
}

