package ssh.dsHive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, date_format, lit}
import ssh.utils.sshUtils._

object cleanPrepare {
    def createTable(spark: SparkSession, dstable:String, dwdTable: String): Unit = {
        val sourceDF = spark.read.table(s"ods.$dstable")

        val finalDF = sourceDF
            .withColumn("dwd_insert_user", lit("user1"))
            .withColumn("dwd_insert_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("dwd_modify_user", lit("user1"))
            .withColumn("dwd_modify_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("etl_date", lit(yesterday))
            .filter(lit(false))

        finalDF.show()

        finalDF.write.mode("overwrite").format("hive").partitionBy("etl_date").saveAsTable(s"dwd.$dwdTable")
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)

        createTable(spark, "user_info", "dim_user_info")
        createTable(spark, "sku_info", "dim_sku_info")
        createTable(spark, "base_province", "dim_province")
        createTable(spark, "base_region", "dim_region")
        createTable(spark, "order_info", "fact_order_info")
        createTable(spark, "order_detail", "fact_order_detail")
        spark.stop()
    }
}
