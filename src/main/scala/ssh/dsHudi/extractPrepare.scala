package ssh.dsHudi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ssh.utils.sshUtils._

import java.util.Properties

object extractPrepare {
    def createTable(spark: SparkSession, mysqlUrl: String, dstable:String, prop: Properties, preCombine: String, Primary: String): Unit = {
        val sourceDF = spark.read.jdbc(mysqlUrl, dstable, prop)
        val addDF = dstable match {
            case "user_info" =>
                sourceDF.withColumn(
                    "operate_time",
                    when(col("operate_time").isNull, col("create_time"))
                        .otherwise(col("operate_time"))
                )

            case "base_province" | "base_region" =>
                sourceDF.withColumn(
                    "create_time",
                    date_trunc("second", current_timestamp())
                )

            case _ =>
                sourceDF
        }

        val finalDF = addDF
            .withColumn("etl_date", lit(yesterday))
            .filter(_ => false)

        val path = s"/user/hive/warehouse/ods_ds_hudi.db/$dstable"

        val hudiOptions = Map(
            "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
            "hoodie.table.name" -> dstable,
            "hoodie.datasource.write.recordkey.field" -> Primary,
            "hoodie.datasource.write.precombine.field" -> preCombine,
            "hoodie.datasource.write.partitionpath.field" -> "etl_date",
            "path" -> path
        )

        finalDF.write
            .mode("overwrite")
            .format("hudi")
            .options(hudiOptions)
            .save()
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)
        val (prop, mysqlUrl) = mysqlConnection("ds_pub", serverIp)

        createTable(spark, mysqlUrl, "user_info", prop, "operate_time", "id")
        createTable(spark, mysqlUrl, "sku_info", prop, "create_time", "id")
        createTable(spark, mysqlUrl, "base_province",prop, "create_time", "id")
        createTable(spark, mysqlUrl, "base_region", prop, "create_time", "id")
        createTable(spark, mysqlUrl, "order_info", prop, "operate_time", "id")
        createTable(spark, mysqlUrl, "order_detail", prop, "create_time", "id")
        spark.stop()
    }
}
