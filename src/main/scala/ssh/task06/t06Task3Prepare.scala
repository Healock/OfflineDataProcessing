package ssh.task06

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import ssh.utils.sshUtils._

import java.sql.Statement

object t06Task3Prepare {
    def createDatabase(stmt: Statement, spark: SparkSession): Unit = {
        stmt.executeUpdate("DROP DATABASE IF EXISTS shtd_result")
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS shtd_result")
        spark.sql(
            """
              |CREATE DATABASE IF NOT EXISTS dws_ds_hudi
              |LOCATION '/user/hive/warehouse/dws_ds_hudi.db'
              |""".stripMargin)
        println("shtd_result dws_ds_hudi database created.")
    }

    def createProvinceConsumptionDayAggr(spark: SparkSession): Unit = {
        // 定义表的 schema，与实际表的字段一致
        val schema = StructType(Seq(
            StructField("uuid", StringType, true),
            StructField("province_id", IntegerType, true),
            StructField("province_name", StringType, true),
            StructField("region_id", IntegerType, true),
            StructField("region_name", StringType, true),
            StructField("total_amount", DoubleType, true),
            StructField("total_count", IntegerType, true),
            StructField("sequence", IntegerType, true),
            StructField("year", IntegerType, true),
            StructField("month", IntegerType, true)
        ))

        // 创建一个空的 DataFrame
        val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

        // 设置 Hudi 表存储路径
        val hudiTablePath = "/user/hive/warehouse/dws_ds_hudi.db/province_consumption_day_aggr"

        // 使用 save 方法将空的 DataFrame 保存为 Hudi 表
        emptyDF.write
            .format("hudi")
            .option("hoodie.datasource.write.recordkey.field", "uuid")  // 设置主键字段
            .option("hoodie.datasource.write.precombine.field", "total_count")  // 设置 precombine 字段
            .option("hoodie.datasource.write.partitionpath.field", "year,month")  // 设置分区字段
            .option("hoodie.table.name", "province_consumption_day_aggr")  // 设置分区字段
            .option("path", hudiTablePath)  // 设置 Hudi 表的存储路径
            .mode("overwrite")  // 如果表已存在则覆盖
            .save()

        // 输出提示信息
        println(s"Empty Hudi table 'province_consumption_day_aggr' created successfully at $hudiTablePath")
    }

    def createTopTen(stmt: Statement): Unit = {
        stmt.executeUpdate(
            """
              |CREATE TABLE shtd_result.topten
              |(
              |    topquantityid Int32,
              |    topquantityname String,
              |    topquantity Int32,
              |    toppriceid String,
              |    toppricename String,
              |    topprice Decimal(18,2),
              |    sequence Int32
              |)
              |ENGINE = MergeTree
              |ORDER BY sequence;
          """.stripMargin
        )
        println("topten table created.")
    }

    def createNationMedian(stmt: Statement): Unit = {
        stmt.executeUpdate(
            """
              |CREATE TABLE shtd_result.nationmedian
              |(
              |    provinceid Int32,
              |    provincename String,
              |    regionid Int32,
              |    regionname String,
              |    provincemedian Float64,
              |    regionmedian Float64
              |)
              |ENGINE = MergeTree
              |ORDER BY (regionid, provinceid);
          """.stripMargin
        )
        println("nationmedian table created.")
    }

    def main(args: Array[String]): Unit = {
        println("t06Task3Prepare...")
        val serverIp = "192.168.45.13"
        val (stmt, connection) = clickhouseConnection(serverIp)
        val spark = sparkConnection(serverIp)

        createDatabase(stmt, spark)
        createProvinceConsumptionDayAggr(spark)
        createTopTen(stmt)
        createNationMedian(stmt)
    }
}
