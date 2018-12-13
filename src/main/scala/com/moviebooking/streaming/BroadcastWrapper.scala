package com.moviebooking.streaming

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

import scala.io.Source
import scala.reflect.ClassTag

// This wrapper lets us update brodcast variables within DStreams' foreachRDD
// without running into serialization issues
case class BroadcastWrapper[T: ClassTag](
                                          @transient private val ssc: SparkContext,
                                          @transient private val _v: String
                                        ) {

  private var time = System.currentTimeMillis()

  @transient private var v = ssc.broadcast(_v)

  private def update(sc: SparkContext, newValue: String ,  blocking: Boolean = false): Unit = {
    v.unpersist(blocking)
    v = sc.broadcast(newValue)
  }

  def value(sc: SparkContext): String = {
    val newTime = System.currentTimeMillis()
    if (newTime - time > Long.MaxValue) {
      time = newTime
      val newcsv= Source.fromFile("/Users/sagarbb/work/my_experiments/sparkstreaming/src/main/resources/customers.csv").mkString
      update(sc, newcsv, true)
    }
    v.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject().asInstanceOf[Broadcast[String]]
  }

}