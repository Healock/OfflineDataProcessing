package ssh.dsHive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ssh.utils.sshUtils._

import java.sql.Timestamp
import java.util.Properties

object extract {
    def dataExtract(spark: SparkSession, dstable: String, prop: Properties, mysqlUrl: String, increment: String): Unit = {
        val odsDF = spark.read.table(s"ods.$dstable")
        val sourceDF = spark.read.jdbc(mysqlUrl, dstable, prop)

        var addDF = dstable match {
            case "base_province" | "base_region" =>
                sourceDF.withColumn(
                    "create_time",
                    lit(date_trunc("second", current_timestamp()))
                )

            case _ =>
                sourceDF
        }

        val incrementDF = dstable match {
            case "base_province" | "base_region" =>
                val maxInt = odsDF
                    .withColumn(increment, col(increment).cast("int"))
                    .select(max(col(increment))).head().getAs[Int](0)
                if (maxInt != null) addDF.filter(col(increment) > maxInt)
                else addDF

            case "user_info" | "order_info" =>
                val maxTime = odsDF
                    .withColumn("biggerTime", greatest(
                        to_timestamp(col("create_time")),
                        to_timestamp(col("operate_time"))
                    ))
                    .select(max("biggerTime")).head().getAs[Timestamp](0)
                if (maxTime != null) addDF.filter(col("create_time") > maxTime || col("operate_time") > maxTime)
                else addDF

            case _ =>
                val maxTime = odsDF
                    .select(max(col(increment))).head().getAs[Timestamp](0)
                if (maxTime != null) addDF.filter(col(increment) > maxTime)
                else addDF
        }

        val finalDF = incrementDF.withColumn("etl_date", lit(yesterday))

        println(s"$dstable finalDF:")
        finalDF.show()

        finalDF.write.mode("append").format("hive").partitionBy("etl_date").saveAsTable(s"ods.$dstable")
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val spark = sparkConnection(serverIp)
        val (prop, mysqlUrl) = mysqlConnection("ds_pub", serverIp)
        dataExtract(spark, "user_info", prop, mysqlUrl, null)
        dataExtract(spark, "sku_info", prop, mysqlUrl, "create_time")
        dataExtract(spark, "base_province",prop, mysqlUrl, "id")
        dataExtract(spark, "base_region", prop, mysqlUrl, "id")
        dataExtract(spark, "order_info", prop, mysqlUrl, null)
        dataExtract(spark, "order_detail", prop, mysqlUrl, "create_time")
        spark.close()
        prop.clear()
    }
}