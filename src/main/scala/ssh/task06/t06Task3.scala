package ssh.task06

import java.util.UUID
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import ssh.utils.sshUtils.{clickhouseConnection, setWarnLog, sparkConnection, writeToClickHouse, writeToHudi}
import ssh.yt.ytTask3.getFactOrderDetail

import java.sql.Statement

object t06Task3 {
    def getFactOrderInfo(spark: SparkSession, columns: Seq[String]): DataFrame = {
        val dwdPath = s"/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info"
        val sourceDF = spark.read.format("hudi").load(dwdPath)

        val factOrderInfo = sourceDF
            .select(columns.map(col): _*)

        factOrderInfo.show(false)

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

    def provinceConsumptionDayAggr(spark: SparkSession): Unit = {
        import spark.implicits._
        val factOrderInfo = getFactOrderInfo(spark, Seq("final_total_amount", "create_time", "province_id"))
        val dimRegion = getDimRegion(spark, Seq("id", "region_name"))
        val dimProvince = getDimProvince(spark, Seq("id", "name", "region_id"))

        val rpDF = dimProvince
            .join(dimRegion, dimProvince("region_id") === dimRegion("id"), "left")
            .select(dimProvince("id").as("province_id").alias("rp_province_id"),
                dimProvince("name").as("province_name"),
                dimProvince("region_id"),
                dimRegion("region_name")
            )

        val finalJoinDF = factOrderInfo
            .join(rpDF, factOrderInfo("province_id") === rpDF("rp_province_id"), "left")

        val generateUUID = udf(() => UUID.randomUUID().toString)

        val df = finalJoinDF
            .withColumn("create_date_str", substring($"create_time", 1, 8))
            .withColumn("create_date", to_date($"create_date_str", "yyyyMMdd"))
            .withColumn("year", year($"create_date"))
            .withColumn("month", month($"create_date"))
            .groupBy("province_id", "province_name", "region_id", "region_name", "year", "month")
            .agg(
                sum("final_total_amount").cast("double").as("total_amount"),
                count("*").cast("int").as("total_count")
            )
            .select("region_id", "region_name", "province_id", "province_name", "total_amount", "total_count", "year", "month")
            .withColumn("uuid", generateUUID())
            .withColumn("sequence", rank().over(Window.orderBy(($"total_amount").desc)))

        df.show(false)

        val path = "/user/hive/warehouse/dws_ds_hudi.db/province_consumption_day_aggr"
        val targetDF = spark.read.format("hudi").load(path)

        writeToHudi(df, targetDF, "dws_ds_hudi", "province_consumption_day_aggr", "uuid", "total_count", "year,month", "overwrite")
    }

    def topTenProducts(spark: SparkSession, stmt: Statement): Unit = {
        import spark.implicits._
        val factOrderDetail = getFactOrderDetail(spark, Seq("order_id", "sku_id", "sku_name", "sku_num", "order_price"))
        val topQuantityDF = factOrderDetail
            .groupBy("sku_id", "sku_name")
            .agg(sum("sku_num").cast("int").as("total_quantity"))
            .orderBy(($"total_quantity").desc)
            .limit(10)

        val topPriceDF = factOrderDetail
            .groupBy("sku_id", "sku_name")
            .agg(sum((($"sku_num") * ($"order_price"))).as("total_price"))
            .orderBy(($"total_price").desc)
            .limit(10)

        val rankedTopQuantity = topQuantityDF
            .withColumn("sequence", row_number().over(Window.orderBy(($"total_quantity").desc)))
            .withColumnRenamed("sku_id", "quantity_sku_id")
            .withColumnRenamed("sku_name", "quantity_sku_name")

        val rankedTopPrice = topPriceDF
            .withColumn("sequence", row_number().over(Window.orderBy(($"total_price").desc)))
            .withColumnRenamed("sku_id", "price_sku_id")
            .withColumnRenamed("sku_name", "price_sku_name")

        val resultDF = rankedTopQuantity.join(rankedTopPrice, Seq("sequence"), "left")
            .select(
                ($"quantity_sku_id").as("topquantityid"),
                ($"quantity_sku_name").as("topquantityname"),
                ($"total_quantity").as("topquantity"),
                ($"price_sku_id").as("toppriceid"),
                ($"price_sku_name").as("toppricename"),
                ($"total_price").as("topprice"),
                ($"sequence")
            )

        writeToClickHouse(resultDF, "shtd_result.topten", stmt)
    }

    def nationMedian(spark: SparkSession, stmt: Statement): Unit = {
        import spark.implicits._
        val factOrderInfo = getFactOrderInfo(spark, Seq("final_total_amount", "create_time", "province_id"))
        val dimRegion = getDimRegion(spark, Seq("id", "region_name"))
        val dimProvince = getDimProvince(spark, Seq("id", "name", "region_id"))

        val rpDF = dimProvince
            .join(dimRegion, dimProvince("region_id") === dimRegion("id"), "left")
            .select(dimProvince("id"), dimProvince("name").as("province_name"), dimRegion("region_name"))

        val finalJoinDF = factOrderInfo
            .join(rpDF, factOrderInfo("province_id") === rpDF("id"), "left")
            .select(
                factOrderInfo("final_total_amount"),
                factOrderInfo("create_time"),
                factOrderInfo("province_id"),
                rpDF("id").as("region_id"),
                rpDF("province_name"),
                rpDF("region_name")
            )

        finalJoinDF.show(false)

        val factWithDateAndLocation = finalJoinDF
            .withColumn("create_date_str", substring($"create_time", 1, 8))
            .withColumn("create_date", to_date($"create_date_str", "yyyyMMdd"))
            .withColumn("year", year($"create_date"))
            .withColumn("month", month($"create_date"))
            .withColumn("regionname", $"region_name")
            .withColumn("regionid", $"region_id")
            .withColumn("provincename", $"province_name")
            .withColumn("provinceid", $"province_id")
            .filter($"year" === 2020)

        val regionMedianDF = factWithDateAndLocation
            .groupBy("regionname", "provincename")
            .agg(
                expr("percentile_approx(order_money, 0.5)").cast("double").as("regionmedian")
            )
            .select("regionid", "regionname", "provinceid", "provincename", "regionmedian")

        val provinceMedianDF = factWithDateAndLocation
            .groupBy("provincename")
            .agg(
                expr("percentile_approx(order_money, 0.5)").cast("double").as("provincemedian")
            )
            .select("provinceid", "provincename", "provincemedian")

        val joinedDF = regionMedianDF
            .join(provinceMedianDF, Seq("provincename"))
            .select("provinceid", "provincename", "regionid", "regionname", "provincemedian", "regionmedian")

        writeToClickHouse(joinedDF, "shtd_result.nationmedian", stmt)
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)

        val (stmt, connection) = clickhouseConnection(serverIp)

        provinceConsumptionDayAggr(spark)

        spark.close()
    }
}
