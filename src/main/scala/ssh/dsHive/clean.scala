package ssh.dsHive

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ssh.utils.sshUtils._

object clean extends Serializable{
    def cleanUpCommonData ( spark:SparkSession,
                            odsTable: String,
                            timeColumns: Seq[String]
                            ): DataFrame = {
        val nowTime = lit(date_trunc("second", current_timestamp()))
        val odsDF = spark.read.table(s"ods.$odsTable")
            .withColumn("dwd_insert_user", lit("user1"))
            .withColumn("dwd_insert_time", nowTime)
            .withColumn("dwd_modify_user", lit("user1"))
            .withColumn("dwd_modify_time", nowTime)

        val formattedTimestampUDF = udf((date: String, format: String) => formatTimestamp(date, format))

        var formattedDF = odsDF
        if (timeColumns != null) {
            for (colName <- timeColumns) {
                formattedDF = formattedDF
                    .withColumn(colName, formattedTimestampUDF(col(colName), lit("yyyy-MM-dd HH:mm:ss")))
                    .withColumn(colName, to_timestamp(col(colName), "yyyy-MM-dd HH:mm:ss"))
            }
        }
        formattedDF
    }

    def cleanUpData(spark:SparkSession,
                    odsTable: String,
                    dwdTable: String,
                    timeColumns: Seq[String],
                    dim: Boolean,
                    mergeKey: Option[String] = None,
                    sortKey: Option[String] = None
                   ): Unit = {
        val commonDF = cleanUpCommonData(spark, odsTable, timeColumns)
        var finalDF = commonDF

        if (dim) {
            val dwdDF = spark.read.table(s"dwd.$dwdTable")
            val unionDF = commonDF.unionByName(dwdDF)

            val resultTimeDF = unionDF.groupBy(mergeKey.get)
                .agg(
                    max("dwd_modify_time").alias("max_dwd_modify_time"),
                    min("dwd_insert_time").alias("min_dwd_insert_time")
                )

            val windowSpec = Window.partitionBy(mergeKey.get).orderBy(sortKey.get)
            val resultDataDF = unionDF
                .withColumn("rank", row_number().over(windowSpec))
                .filter(col("rank") === 1)
                .drop("rank")

            finalDF = resultDataDF
                .join(resultTimeDF, Seq(mergeKey.get), "inner")
                .withColumn("dwd_modify_time", col("max_dwd_modify_time"))
                .withColumn("dwd_insert_time", col("min_dwd_insert_time"))
                .drop("max_dwd_modify_time", "min_dwd_insert_time")
        }

        odsTable match {
            case "order_info" | "user_info" =>
                finalDF = finalDF.withColumn(
                    "operate_time", when(col("operate_time").isNull, col("create_time")).otherwise(col("operate_time"))
                )

            case "order_info" | "order_detail" =>
                finalDF = finalDF.withColumn("etl_date", date_format(col("create_time"), "yyyyMMdd"))

            case _ => finalDF
        }

        val targetDF = spark.table(s"dwd.$dwdTable")

        finalDF.select(targetDF.columns.map(col): _*)
            .write.mode("overwrite").format("hive").insertInto(s"dwd.$dwdTable")
    }

    def main(args: Array[String]): Unit = {
        val spark = sparkConnection(serverIp)
        cleanUpData(spark,
            odsTable = "user_info",
            dwdTable = "dim_user_info",
            timeColumns = Seq("create_time", "operate_time"),
            dim = true,
            mergeKey = Some("id"),
            sortKey = Some("operate_time")
        )

        cleanUpData(spark,
            odsTable = "sku_info",
            dwdTable = "dim_sku_info",
            timeColumns = Seq("create_time"),
            dim = true,
            mergeKey = Some("id"),
            sortKey = Some("create_time")
        )

        cleanUpData(spark,
            odsTable = "base_province",
            dwdTable = "dim_province",
            timeColumns = null,
            dim = true,
            mergeKey = Some("id"),
            sortKey = Some("create_time")
        )

        cleanUpData(spark,
            odsTable = "base_region",
            dwdTable = "dim_region",
            timeColumns = null,
            dim = true,
            mergeKey = Some("id"),
            sortKey = Some("create_time")
        )

        cleanUpData(spark,
            odsTable = "order_info",
            dwdTable = "fact_order_info",
            timeColumns = Seq("operate_time", "expire_time"),
            dim = false
        )

        cleanUpData(spark,
            odsTable = "order_detail",
            dwdTable = "fact_order_detail",
            timeColumns = null,
            dim = false
        )

        spark.close()
    }
}
