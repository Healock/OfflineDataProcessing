package ssh.task16

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import ssh.utils.sshUtils._

object t16Task2 {
    def cleanUpInfData( spark: SparkSession, odsTableName: String,dwdTableName: String, mergeKey: String, timeColumns: Seq[String]): Unit = {

        import spark.implicits._

        val nowTime = lit(date_trunc("second", current_timestamp()))

        val odsDF = spark.table(odsTableName)
            .filter(($"etl_date") === getYesterdayDate)
            .withColumn("dwd_insert_user", lit("user1"))
            .withColumn("dwd_insert_time", nowTime)
            .withColumn("dwd_modify_user", lit("user1"))
            .withColumn("dwd_modify_time", nowTime)

        val formatTimestampUDF = udf((date: String, format: String) => formatTimestamp(date, format))

        var formattedOdsDF = odsDF
        for (colName <- timeColumns) {
            formattedOdsDF = formattedOdsDF
                .withColumn(colName, formatTimestampUDF(col(colName), lit("yyyy-MM-dd HH:mm:ss")))
                .withColumn(colName, to_timestamp(col(colName), "yyyy-MM-dd HH:mm:ss"))
        }

        val dwdDF = spark.table(dwdTableName)
            .filter(($"etl_date") === getYesterdayDate)

        val unionDF = formattedOdsDF
            .unionByName(dwdDF)

        val resultTimeDF = unionDF.groupBy(mergeKey)
            .agg(
                max("dwd_modify_time").alias("new_dwd_modify_time"),
                min("dwd_insert_time").alias("new_dwd_insert_time")
            )

        val windowSpec = Window.partitionBy(mergeKey).orderBy(($"modified_time").desc)

        val resultDataDF = unionDF
            .withColumn("ranking", row_number().over(windowSpec))
            .filter(($"ranking") === 1)
            .drop("ranking")

        val dwdUpdateDF = resultDataDF
            .join(resultTimeDF, Seq(mergeKey), "inner")
            .withColumn("dwd_modify_time", ($"new_dwd_modify_time"))
            .withColumn("dwd_insert_time", ($"new_dwd_insert_time"))
            .drop("new_dwd_modify_time")
            .drop("new_dwd_insert_time")

        val df = spark.read.table(dwdTableName)

        val finalDF = dwdUpdateDF.select(df.columns.map(col): _*)

        finalDF.show(false)

        finalDF.write
            .mode("overwrite")
            .insertInto(dwdTableName)
    }

    def cleanUpOrderData(
                            spark: SparkSession,
                            odsTableName: String,
                            dwdTableName: String,
                            timeColumns: Seq[String]
                        ): Unit = {
        import spark.implicits._

        val nowTime = lit(date_trunc("second", current_timestamp()))

        val odsDF = spark.table(odsTableName)
            .withColumn("etl_date", date_format(col("create_time"), "yyyyMMdd"))
            .withColumn("dwd_insert_user", lit("user1"))
            .withColumn("dwd_insert_time", nowTime)
            .withColumn("dwd_modify_user", lit("user1"))
            .withColumn("dwd_modify_time", nowTime)

        val formatTimestampUDF = udf((date: String, format: String) => formatTimestamp(date, format))

        var formattedOdsDF = odsDF
        for (colName <- timeColumns) {
            formattedOdsDF = formattedOdsDF
                .withColumn(colName, formatTimestampUDF(col(colName), lit("yyyy-MM-dd HH:mm:ss")))
                .withColumn(colName, to_timestamp(col(colName), "yyyy-MM-dd HH:mm:ss"))
        }

        val createTimeDF = formattedOdsDF.withColumn("create_time", formatTimestampUDF(col("create_time"), lit("yyyyMMdd")))
        println("createTimeDF show:")
        createTimeDF.show(false)

        val filterCityDF = if (odsTableName == "ods.order_master") {
            createTimeDF
                .filter(length(($"city")) <= 8)
        } else {
            createTimeDF
        }

        val df = spark.read.table(dwdTableName)

        val finalDF = filterCityDF.select(df.columns.map(col): _*)

        finalDF.show(false)

        finalDF.write
            .mode("overwrite")
            .insertInto(dwdTableName)
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()

        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)

        spark.sql("use ods")

        setDynamicPartitionConfig(spark)

        cleanUpInfData(
            spark,
            odsTableName = "ods.customer_inf",
            dwdTableName = "dwd.dim_customer_inf",
            mergeKey = "customer_id",
            timeColumns = Seq("register_time", "modified_time")
        )

        cleanUpInfData(
            spark,
            odsTableName = "ods.product_info",
            dwdTableName = "dwd.dim_product_info",
            mergeKey = "product_id",
            timeColumns = Seq("production_date", "indate", "modified_time")
        )

        cleanUpOrderData(
            spark,
            odsTableName = "ods.order_detail",
            dwdTableName = "dwd.fact_order_detail",
            timeColumns = Seq("modified_time")
        )

        cleanUpOrderData(
            spark,
            odsTableName = "ods.order_master",
            dwdTableName = "dwd.fact_order_master",
            timeColumns = Seq("modified_time","shipping_time", "pay_time", "receive_time")
        )

        stopAll(spark,statement = null,connection = null,prop = null)
    }
}