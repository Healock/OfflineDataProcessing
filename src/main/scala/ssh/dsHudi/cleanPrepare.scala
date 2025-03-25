package ssh.dsHudi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, date_format, lit}
import ssh.utils.sshUtils._

object cleanPrepare {
    def createTable(spark: SparkSession, dstable:String, dwdTable: String, preCombine: String, Primary: String): Unit = {
        val odsPath = s"/user/hive/warehouse/ods_ds_hudi.db/$dstable"
        val sourceDF = spark.read.format("hudi").load(odsPath)

        val finalDF = sourceDF
            .withColumn("dwd_insert_user", lit("user1"))
            .withColumn("dwd_insert_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("dwd_modify_user", lit("user1"))
            .withColumn("dwd_modify_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("etl_date", lit(yesterday))
            .filter(lit(false))

        finalDF.show()

        val path = s"/user/hive/warehouse/dwd_ds_hudi.db/$dwdTable"

        val hudiOptions = Map(
            "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
            "hoodie.table.name" -> dwdTable,
            "hoodie.datasource.write.recordkey.field" -> Primary,
            "hoodie.datasource.write.precombine.field" -> preCombine,
            "hoodie.datasource.write.partitionpath.field" -> "etl_date",
            "path" -> path
        )

        finalDF.write.mode("overwrite").format("hudi").options(hudiOptions).save()
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)

        createTable(spark, "user_info", "dim_user_info", "operate_time", "id")
        createTable(spark, "sku_info", "dim_sku_info", "dwd_modify_time", "id")
        createTable(spark, "base_province", "dim_province", "dwd_modify_time", "id")
        createTable(spark, "base_region", "dim_region", "dwd_modify_time", "id")
        createTable(spark, "order_info", "fact_order_info", "operate_time", "id")
        createTable(spark, "order_detail", "fact_order_detail", "dwd_modify_time", "id")
        spark.stop()
    }
}
