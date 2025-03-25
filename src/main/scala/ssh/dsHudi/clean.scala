package ssh.dsHudi

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ssh.utils.sshUtils._

object clean {
    def cleanUpCommonData(  spark: SparkSession,
                            odsTable: String,
                            timeColumns: Seq[String]
                        ): DataFrame = {
        val nowTime = lit(date_trunc("second", current_timestamp()))

        val odsDF = readFromHudi(spark, "ods_ds_hudi", odsTable)
            .withColumn("dwd_insert_user", lit("user1"))
            .withColumn("dwd_insert_time", nowTime)
            .withColumn("dwd_modify_user", lit("user1"))
            .withColumn("dwd_modify_time", nowTime)

        val formatTimestampUDF = udf((date: String, format: String) => formatTimestamp(date, format))

        var formattedDF = odsDF
        if (timeColumns != null) {
            for (colName <- timeColumns) {
                formattedDF = formattedDF
                    .withColumn(colName, formatTimestampUDF(col(colName), lit("yyyy-MM-dd HH:mm:ss")))
                    .withColumn(colName, to_timestamp(col(colName), "yyyy-MM-dd HH:mm:ss"))
            }
        }
        formattedDF
    }

    def cleanUpData(spark: SparkSession,
                    odsTable: String,
                    dwdTable: String,
                    timeColumns: Seq[String],
                    primary: String,
                    preCombine: String,
                    dim: Boolean,
                    mergeKey: Option[String] = None,
                    sortKey: Option[String] = None
                    ): Unit = {
        import spark.implicits._

        val commonDF = cleanUpCommonData(spark, odsTable, timeColumns)
        var finalDF = commonDF

        if (dim) {
            val dwdDF = readFromHudi(spark, "dwd_ds_hudi", dwdTable)

            val unionDF = commonDF.unionByName(dwdDF)

            val resultTimeDF = unionDF.groupBy(mergeKey.get)
                .agg(
                    max("dwd_modify_time").alias("max_dwd_modify_time"),
                    min("dwd_insert_time").alias("min_dwd_insert_time")
                )

            val windowSpec = Window.partitionBy(mergeKey.get).orderBy(col(sortKey.get).desc)
            val resultDataDF = unionDF
                .withColumn("rank", row_number().over(windowSpec))
                .filter($"rank" === 1)
                .drop("rank")

            finalDF = resultDataDF
                .join(resultTimeDF, Seq(mergeKey.get), "inner")
                .withColumn("dwd_modify_time", $"max_dwd_modify_time")
                .withColumn("dwd_insert_time", $"min_dwd_insert_time")
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

        val targetDF = readFromHudi(spark, "dwd_ds_hudi", dwdTable)
        writeToHudi(finalDF, targetDF, "dwd_ds_hudi", dwdTable, primary, preCombine, partition = "etl_date", "overwrite")
        spark.sql(s"msck repair table dwd_ds_hudi.$dwdTable")
    }

    def showTables(spark: SparkSession): Unit = {
        val tables = List("dim_user_info", "dim_sku_info", "dim_province", "dim_region", "fact_order_info", "fact_order_detail")
        def showTable(tableName: String): Unit = {
            val dwdDF = readFromHudi(spark, "dwd_ds_hudi", tableName)
            println(s"Showing data from table: $tableName")
            dwdDF.show(false)
        }

        tables.foreach {
            case "dim_user_info" =>
                showTable("dim_user_info")
            case "dim_sku_info" =>
                showTable("dim_sku_info")
            case "dim_province" =>
                showTable("dim_province")
            case "dim_region" =>
                showTable("dim_region")
            case "fact_order_info" =>
                showTable("fact_order_info")
            case "fact_order_detail" =>
                showTable("fact_order_detail")
            case _ =>
                println("Unknown table")
        }
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)
        cleanUpData(spark,
            odsTable = "user_info",
            dwdTable = "dim_user_info",
            timeColumns = Seq("create_time", "operate_time"),
            primary = "id",
            preCombine = "operate_time",
            dim = true,
            mergeKey = Some("id"),
            sortKey = Some("operate_time")
        )

        cleanUpData(spark,
            odsTable = "sku_info",
            dwdTable = "dim_sku_info",
            timeColumns = Seq("create_time"),
            primary = "id",
            preCombine = "dwd_modify_time",
            dim = true,
            mergeKey = Some("id"),
            sortKey = Some("create_time")
        )

        cleanUpData(spark,
            odsTable = "base_province",
            dwdTable = "dim_province",
            timeColumns = null,
            primary = "id",
            preCombine = "dwd_modify_time",
            dim = true,
            mergeKey = Some("id"),
            sortKey = Some("create_time")
        )

        cleanUpData(spark,
            odsTable = "base_region",
            dwdTable = "dim_region",
            timeColumns = null,
            primary = "id",
            preCombine = "dwd_modify_time",
            dim = true,
            mergeKey = Some("id"),
            sortKey = Some("create_time")
        )

        cleanUpData(spark,
            odsTable = "order_info",
            dwdTable = "fact_order_info",
            timeColumns = Seq("operate_time", "expire_time"),
            primary = "id",
            preCombine = "operate_time",
            dim = false
        )

        cleanUpData(spark,
            odsTable = "order_detail",
            dwdTable = "fact_order_detail",
            timeColumns = null,
            primary = "id",
            preCombine = "dwd_modify_time",
            dim = false
        )

        showTables(spark)
        stopAll(spark, null, null, null)
    }
}
