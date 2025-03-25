package ssh.task07

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ssh.utils.sshUtils._

object t07Task3 {
    def getTable(spark: SparkSession, table: String, columns: Seq[String]): DataFrame = {
        val sourceDF = spark
            .table(s"dwd.$table")

        val df = sourceDF
            .select(columns.map(col): _*)

        df
    }

    def provinceConsumptionDayAggr(spark: SparkSession): Unit = {
        import spark.implicits._
        val factOrderInfo = getTable(spark, "fact_order_info", Seq("final_total_amount", "create_time", "province_id"))
        val dimRegion = getTable(spark, "dim_region", Seq("id", "region_name"))
        val dimProvince = getTable(spark, "dim_province", Seq("id", "name", "region_id"))

        /* --------------- 取出数据 --------------- */

        val dimProvinceRenamed = dimProvince
            .withColumnRenamed("id", "province_id")
            .withColumnRenamed("name", "province_name")
        val dimRegionRenamed = dimRegion
            .withColumnRenamed("id", "region_id")

        val factWithProvince = factOrderInfo.join(dimProvinceRenamed, Seq("province_id"), "left")

        val finalJoinDF = factWithProvince.join(dimRegionRenamed, Seq("region_id"), "left")
            .select("province_id", "province_name", "region_id", "region_name", "final_total_amount", "create_time")

        /* --------------- 汇总数据 --------------- */

        val finalDF = finalJoinDF
            .withColumn("create_date", to_date(substring($"create_time", 1, 10), "yyyy-MM-dd"))
            .withColumn("year", year($"create_date"))
            .withColumn("month", month($"create_date"))
            .groupBy("province_id", "province_name", "region_id", "region_name", "year", "month")
            .agg(
                sum("final_total_amount").cast("double").as("total_amount"),
                count("*").cast("int").as("total_count")
            )
            .select("region_id", "region_name", "province_id", "province_name", "total_amount", "total_count", "year", "month")
            .withColumn("sequence", rank().over(Window.orderBy(($"total_amount").desc)))

        writeToHive(finalDF, "overwrite", "dws.province_consumption_day_aggr", Seq("year", "month"))

        /* --------------- 计算并存表 --------------- */

        spark.sql(
            """
            |SELECT province_id, province_name, region_id, region_name, CAST(total_amount AS BIGINT) AS total_amount, total_count, sequence, year, month
            |FROM dws.province_consumption_day_aggr
            |ORDER BY total_count DESC, total_amount DESC, province_id DESC
            |LIMIT 5
            """.stripMargin).show(truncate = false)

        /* --------------- 查询 --------------- */
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val spark = sparkConnection(serverIp)
        val objectName = this.getClass.getSimpleName
        println(s"$objectName...")

        provinceConsumptionDayAggr(spark)

    }
}
