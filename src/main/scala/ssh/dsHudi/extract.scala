package ssh.dsHudi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ssh.utils.sshUtils._

import java.sql.Timestamp
import java.util.Properties

object extract {
    def dataExtract(spark: SparkSession, dstable: String, prop: Properties, mysqlUrl: String,
                    increment: String = null,
                    primary: String,
                    preCombine: String): Unit = {
        import spark.implicits._

        val odsDF = readFromHudi(spark, "ods_ds_hudi", dstable)
        val sourceDF = spark.read.jdbc(mysqlUrl, dstable, prop)

        val addDF = dstable match {
            case "user_info" =>
                sourceDF.withColumn(
                    "operate_time",
                    when($"operate_time".isNull, $"create_time")
                        .otherwise($"operate_time")
                )

            case "base_province" | "base_region" =>
                sourceDF.withColumn(
                    "create_time",
                    date_trunc("second", current_timestamp())
                )

            case _ =>
                sourceDF
        }

        val incrementDF = dstable match {
            case "base_province" | "base_region" =>
                val maxInt = odsDF
                    .withColumn(increment, col(increment).cast("int"))
                    .select(max(col(increment))).head().getAs[Int](0)
                if (maxInt != null) addDF.filter(
                    col(increment) > maxInt
                ) else addDF

            case "user_info" | "order_info" =>
                val maxTime = odsDF
                    .withColumn("biggerTime", greatest(
                        to_timestamp(col("create_time")),
                        to_timestamp(col("operate_time"))
                    ))
                    .select(max("biggerTime")).head().getAs[Timestamp](0)
                if (maxTime != null) addDF.filter(
                    col("create_time") > maxTime || col("operate_time") > maxTime
                ) else addDF

            case _ =>
                val maxTime = odsDF
                    .select(max(col(increment))).head().getAs[Timestamp](0)
                if (maxTime != null) addDF.filter(
                    col(increment) > maxTime
                ) else addDF
        }

        val finalDF = incrementDF.withColumn("etl_date", lit(yesterday))

        writeToHudi(finalDF, odsDF, "ods_ds_hudi", dstable, primary, preCombine, "etl_date", "overwrite")
        spark.sql(s"msck repair table ods_ds_hudi.$dstable")
    }

    def showTables(spark: SparkSession): Unit = {
        val tables = List("user_info", "sku_info", "base_province", "base_region", "order_info", "order_detail")
        def showTable(tableName: String): Unit = {
            val path = s"/user/hive/warehouse/ods_ds_hudi.db/$tableName/"
            val odsDF = spark.read.format("hudi").load(path)
            println(s"Showing data from table: $tableName")
            odsDF.show(false)
        }

        tables.foreach {
            case "user_info" =>
                showTable("user_info")
            case "sku_info" =>
                showTable("sku_info")
            case "base_province" =>
                showTable("base_province")
            case "base_region" =>
                showTable("base_region")
            case "order_info" =>
                showTable("order_info")
            case "order_detail" =>
                showTable("order_detail")
            case _ =>
                println("Unknown table")
        }
    }


    def main(args: Array[String]): Unit = {
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)
        val (prop, mysqlUrl) = mysqlConnection("ds_pub", serverIp)
        dataExtract(spark, "user_info", prop, mysqlUrl, null, "id", "operate_time")
        dataExtract(spark, "sku_info", prop, mysqlUrl, "create_time", "id", "create_time")
        dataExtract(spark, "base_province", prop, mysqlUrl, "id", "id", "create_time")
        dataExtract(spark, "base_region", prop, mysqlUrl, "id", "id", "create_time")
        dataExtract(spark, "order_info", prop, mysqlUrl, null, "id", "operate_time")
        dataExtract(spark, "order_detail", prop, mysqlUrl, "create_time", "id", "create_time")
        showTables(spark)
        stopAll(spark, statement = null, connection = null, prop)
    }
}
