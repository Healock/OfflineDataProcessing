package ssh.yt

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ssh.utils.sshUtils._

import java.sql.Statement

object ytTask3 {
    def prepare(stmt: Statement): Unit = {
        def createShtdResultDatabase(stmt: Statement): Unit = {
            stmt.executeUpdate("DROP DATABASE IF EXISTS shtd_result")
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS shtd_result")
            println("shtd_result database created.")
        }

        def createTopTenTable(stmt: Statement): Unit = {
            stmt.executeUpdate(
                """
                  |CREATE TABLE IF NOT EXISTS shtd_result.topten (
                  |  topquantityid Int32,
                  |  topquantityname String,
                  |  topquantity Int32,
                  |  toppriceid String,
                  |  toppricename String,
                  |  topprice Decimal(10, 2),
                  |  sequence Int32
                  |) ENGINE = MergeTree()
                  |ORDER BY topquantityid
          """.stripMargin
            )
            println("topten table created.")
        }

        def createNationMedianTable(stmt: Statement): Unit = {
            stmt.executeUpdate(
                """
                  |CREATE TABLE IF NOT EXISTS shtd_result.nationmedian (
                  |  provinceid Int32,
                  |  provincename String,
                  |  regionid Int32,
                  |  regionname String,
                  |  provincemedian Float64,
                  |  regionmedian Float64
                  |) ENGINE = MergeTree()
                  |ORDER BY cityname
          """.stripMargin
            )
            println("citymidcmpprovince table created.")
        }

        createShtdResultDatabase(stmt)
        createTopTenTable(stmt)
        createNationMedianTable(stmt)
    }

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

    def topTen(spark: SparkSession, stmt: Statement): Unit = {
        import spark.implicits._
        val factOrderDetail = getFactOrderDetail(spark, Seq("order_id", "sku_id", "sku_name", "sku_num", "order_price"))
        val topQuantityDF = factOrderDetail
            .groupBy("sku_id", "sku_name")
            .agg(sum("sku_num").cast("int").as("total_quantity"))
            .orderBy($"total_quantity".desc)
            .limit(10)

        val topPriceDF = factOrderDetail
            .groupBy("sku_id", "sku_name")
            .agg(sum(($"sku_num") * ($"order_price")).as("total_price"))
            .orderBy($"total_price".desc)
            .limit(10)

        val rankedTopQuantity = topQuantityDF
            .withColumn("sequence", row_number().over(Window.orderBy($"total_quantity".desc)))
            .withColumnRenamed("sku_id", "quantity_sku_id")
            .withColumnRenamed("sku_name", "quantity_sku_name")

        val rankedTopPrice = topPriceDF
            .withColumn("sequence", row_number().over(Window.orderBy($"total_price".desc)))
            .withColumnRenamed("sku_id", "price_sku_id")
            .withColumnRenamed("sku_name", "price_sku_name")

        val resultDF = rankedTopQuantity.join(rankedTopPrice, Seq("sequence"), "left")
            .select(
                $"quantity_sku_id".as("topquantityid"),
                $"quantity_sku_name".as("topquantityname"),
                $"total_quantity".as("topquantity"),
                $"price_sku_id".as("toppriceid"),
                $"price_sku_name".as("toppricename"),
                $"total_price".as("topprice"),
                $"sequence"
            )

        writeToClickHouse(resultDF, "shtd_result.topten", stmt)
    }

    def main(args: Array[String]): Unit = {
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)

        val (stmt, connection) = clickhouseConnection(serverIp)
        prepare(stmt)

        topTen(spark, stmt)
    }
}
