package ssh.task16

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import ssh.utils.sshUtils._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object t16Task2Prepare extends Serializable {

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)

        val serverIp = "192.168.45.13"
        val config = new SparkConf()
            .setAppName("dataExtract")
            .setMaster(s"yarn")
            .set("spark.sql.warehouse.dir", s"hdfs://$serverIp:9000/user/hive/warehouse")
            .set("spark.sql.metastore.uris", s"thrift://$serverIp:9083")

        val spark = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()

        setDynamicPartitionConfig(spark)

        cleanTable(
            spark,
            odsTableName = "ods.customer_inf",
            dwdTableName = "dim_customer_inf"
        )

        cleanTable(
            spark,
            odsTableName = "ods.product_info",
            dwdTableName = "dim_product_info"
        )

        cleanTable(
            spark,
            odsTableName = "ods.order_detail",
            dwdTableName = "fact_order_detail"
        )

        cleanTable(
            spark,
            odsTableName = "ods.order_master",
            dwdTableName = "fact_order_master"
        )

        spark.stop()
    }

    def cleanTable(
                      spark: SparkSession,
                      odsTableName: String,
                      dwdTableName: String
                  ): Unit = {
        spark.sql("use dwd")

        spark.sql(s"DROP TABLE IF EXISTS $dwdTableName")

        spark.sql(
            s"""
               |CREATE TABLE $dwdTableName LIKE $odsTableName
               |""".stripMargin)
        spark.sql(
            s"""
               |ALTER TABLE $dwdTableName
               |ADD COLUMNS (
               |dwd_insert_user STRING,
               |dwd_insert_time TIMESTAMP,
               |dwd_modify_user STRING,
               |dwd_modify_time TIMESTAMP)
               |""".stripMargin)

        println(s"ssh:------ ${spark.table(dwdTableName).count()} -------")
    }
}
