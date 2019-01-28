package com.log.ingestion

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import java.text.{ DateFormat, SimpleDateFormat }
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.Session
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector._
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.driver.core.utils.UUIDs

object LogsIngestion extends Serializable {


  def main(args: Array[String]) {

    try {

      val cols = List("requestid", "hostname", "datetime", "requesttype", "requestpage", "responsestatus")
      val host = "localhost"
      val keyspace = "perfmonitor"
      val clusterName = "Test Cluster"
      val tableName = "logingestion"

      val sparkSession = SparkSession.builder
        .master("local[2]")
        .appName("LogIngetsion")
        .config("spark.cassandra.connection.host", host)
        .getOrCreate()

      val lines = sparkSession.readStream
        .format("kafka")
        .option("subscribe", "LogAnalytics")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr(
          "CAST(value AS STRING)",
          "CAST(topic as STRING)",
          "CAST(partition as INTEGER)")

      //lines.printSchema

      import sparkSession.implicits._

      val df = lines.map(line => line.get(0).toString().split(",")) // split the value by comma  //getTimeStamp(l(0))
        .filter(arr => arr.length == 4) // ensure that the array has at least 4 values to be correct and complete message
        .map(l => (UUIDs.timeBased().toString, l(0), l(1), l(2).split(" ")(0), l(2).split(" ")(1).split(" ")(0), l(3))).toDF(cols: _*) // convert the array to dataframe using the columns list

      // df.printSchema()

      val query =
        df.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          batchDF.write // Use Cassandra batch data source to write streaming out
            .cassandraFormat(tableName, keyspace)
            .option("cluster", clusterName)
            .mode("append")
            .save()
        }
          .outputMode("update")
          .start()

      query.awaitTermination()
      sparkSession.stop()

      //Use the below if you want to print the messages in the eclipse console to ensure that Kafka is working fine first.
      // val query = df.writeStream
      //      .format("console")
      //      .start()
      //    query.awaitTermination()
      //    sparkSession.stop()

      
    } catch {
      case ex: Exception =>
        ex.getStackTrace
    }

  }
}
