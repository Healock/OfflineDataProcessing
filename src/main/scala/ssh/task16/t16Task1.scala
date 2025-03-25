package ssh.task16

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ssh.utils.sshUtils._

import java.sql.Timestamp
import java.util.Properties

object t16Task1 {
    def extractData(spark: SparkSession, dstable: String, mysqlUrl: String, prop: Properties): Unit = {
        import spark.implicits._
        val hiveDF = spark.read.table(dstable)
        val maxTime = hiveDF.agg(max("modified_time").alias("max_modified_time")).head.getAs[Timestamp]("max_modified_time")
        val df = spark.read.jdbc(mysqlUrl, dstable, prop)
        val incrementDF = if (maxTime != null) df.filter($"modified_time" > lit(maxTime)) else df
        val finalDF = incrementDF.withColumn("etl_date", lit(getYesterdayDate))
        finalDF.write.mode("append").format("hive").partitionBy("etl_date").saveAsTable(dstable)
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)
        val (prop, mysqlUrl) = mysqlConnection("ds_db01", serverIp)
        spark.sql("USE ods")
        extractData(spark, "customer_inf", mysqlUrl, prop)
        extractData(spark, "product_info", mysqlUrl, prop)
        extractData(spark, "order_detail", mysqlUrl, prop)
        extractData(spark, "order_master", mysqlUrl, prop)
        spark.stop()
    }
}