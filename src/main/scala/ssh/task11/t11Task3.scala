package ssh.task11

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ssh.utils.sshUtils._

import java.sql.Statement
import java.util.{Properties, UUID}

object t11Task3 {
    def getFactOrderInfo(spark: SparkSession, columns: Seq[String]): DataFrame = {
        val dwdPath = s"/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info"
        val sourceDF = spark.read.format("hudi").load(dwdPath)

        val factOrderInfo = sourceDF
            .select(columns.map(col): _*)

        factOrderInfo
    }
    def getFactOrderDetail(spark: SparkSession, columns: Seq[String]): DataFrame = {
        val dwdPath = s"/user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail"
        val sourceDF = spark.read.format("hudi").load(dwdPath)

        val factOrderDetail = sourceDF
            .select(columns.map(col): _*)

        factOrderDetail
    }
    def getDimProvince(spark: SparkSession, columns: Seq[String]): DataFrame = {
        val dwdPath = s"/user/hive/warehouse/dwd_ds_hudi.db/dim_province"
        val sourceDF = spark.read.format("hudi").load(dwdPath)

        val dimProvince = sourceDF
            .select(columns.map(col): _*)

        dimProvince
    }
    def getDimRegion(spark: SparkSession, columns: Seq[String]): DataFrame = {
        val dwdPath = s"/user/hive/warehouse/dwd_ds_hudi.db/dim_region"
        val sourceDF = spark.read.format("hudi").load(dwdPath)

        val dimRegion = sourceDF
            .select(columns.map(col): _*)

        dimRegion
    }

    def getDimUserInfo(spark: SparkSession, columns: Seq[String]): DataFrame = {
        val dwdPath = s"/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info"
        val sourceDF = spark.read.format("hudi").load(dwdPath)

        val dimUserInfo = sourceDF
            .select(columns.map(col): _*)

        dimUserInfo
    }

    def userConsumptionDayAggr(spark: SparkSession, prop: Properties, mysqlUrl: String): Unit = {
        import spark.implicits._

        val factOrderInfo = getFactOrderInfo(spark, Seq("user_id", "final_total_amount", "create_time"))

        val dimUserInfo = getDimUserInfo(spark, Seq("id", "name"))

        val generateUUID = udf(() => UUID.randomUUID().toString)

        val factWithDate = factOrderInfo
            .withColumn("uuid", generateUUID())
            .withColumn("create_date", to_date(substring($"create_time", 1, 10), "yyyy-MM-dd"))
            .withColumn("year", year($"create_date"))
            .withColumn("month", month($"create_date"))
            .withColumn("day", dayofmonth($"create_date"))

        val joinedDF = factWithDate.join(dimUserInfo, Seq("id"), "inner").cache()

        val aggregatedDF = joinedDF.groupBy("id", "name", "year", "month", "day")
            .agg(
                count("*").cast("int").as("total_count"),
                sum("order_money").cast("double").as("total_amount")
            )

        val finalDF = aggregatedDF
            .withColumnRenamed("id", "user_id")
            .withColumnRenamed("name", "user_name")

        writeToMySQL(finalDF, "overwrite", "shtd_result", prop, mysqlUrl)
    }

    def provinceConsumptionDayAggr(spark: SparkSession, prop: Properties, mysqlUrl: String): Unit = {}

    def maxAvgOrderPriceSeq(spark: SparkSession, prop: Properties, mysqlUrl: String): Unit ={
        import spark.implicits._
        val sourceDF = getFactOrderInfo(spark, Seq("id", "final_total_amount"))
        val row0 = Seq((3442, 0.0)).toDF("id", "final_total_amount")

        val operation1 = sourceDF
            .unionByName(row0)
            .orderBy("id")
            .filter($"id" <= 20000)

        println("operation1:")
        operation1.show(false)

        val windowSpec = Window.orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        val operation2 = operation1
            .withColumn("sum", sum($"final_total_amount").over(windowSpec))

        println("operation2:")
        operation2.show(false)

        val operation3 = operation2.as("left")
            .crossJoin(operation2.as("right"))
            .select(
                col("left.id").alias("left_id"),
                col("left.final_total_amount").alias("left_amount"),
                col("left.sum").alias("left_sum"),
                col("right.id").alias("right_id"),
                col("right.final_total_amount").alias("right_amount"),
                col("right.sum").alias("right_sum")
            )
            .filter((col("right_id") - col("left_id")) >= 20)

        println("operation3:")
        operation3.show(3000)

    }


    def main(args: Array[String]): Unit = {
        val objectName = this.getClass.getSimpleName
        println(s"$objectName...")
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)
        val (prop, mysqlUrl) = mysqlConnection("shtd_result", serverIp)
        val (stmt, connection) = clickhouseConnection(serverIp)

        maxAvgOrderPriceSeq(spark, prop, mysqlUrl)

        stopAll(spark, stmt, connection, prop)
    }
}
