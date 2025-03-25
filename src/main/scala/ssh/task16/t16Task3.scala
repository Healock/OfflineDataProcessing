package ssh.task16

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import ssh.utils.sshUtils._

import java.sql.{Connection, DriverManager, Statement}

object t16Task3 {
    def getFactOrderMaster(spark: SparkSession, columns: Seq[String]): DataFrame = {
        import spark.implicits._

        // 直接用 Dataframe 读取表，并筛选得到 order_status 已退款之外的行
        // isin 用法检查是否在某个集合中，返回boolean
        val sourceDF = spark
            .table("dwd.fact_order_master")
            .filter(!col("order_status").isin("已退款"))
            .cache()

        // 窗口规范 order_sn 按分区分组，create_time 倒序
        val windowSpec = Window.partitionBy("order_sn").orderBy($"create_time".desc)

        // 从数据中检查得知 create_time 在相同订单内是相同的，所以只需要保留第一个
        // 相同订单内产生 1、2、3、4 的 row_num，filter 只保留第一行
        // 这样只保留了 row_num 为 1 的行，即为唯一数据实现去重
        val factOrderMaster = sourceDF
            .withColumn("row_num", row_number().over(windowSpec))
            .filter($"row_num" === 1)
            // 可变参数，传入columns，map展开成列名
            .select(columns.map(col): _*)

        factOrderMaster
    }

    def getDimCustomerInf(spark: SparkSession, columns: Seq[String]): DataFrame = {
        val sourceDF = spark
            .table("dwd.dim_customer_inf")
            .cache()

        val DimCustomerInf = sourceDF
            .select(columns.map(col): _*)

        DimCustomerInf
    }

    def getDimProductInfo(spark: SparkSession, columns: Seq[String]): DataFrame = {
        val sourceDF = spark
            .table("dwd.dim_product_info")
            .cache()

        val DimProductInfo = sourceDF
            .select(columns.map(col): _*)

        DimProductInfo
    }

    def getFactOrderDetail(spark: SparkSession, columns: Seq[String]): DataFrame = {
        val sourceDF = spark
            .table("dwd.fact_order_detail")
            .cache()

        val FactOrderDetail = sourceDF
            .select(columns.map(col): _*)

        FactOrderDetail
    }

    def userConsumptionDayAggr(spark: SparkSession): Unit = {
        setDynamicPartitionConfig(spark)
        import spark.implicits._

        val factOrderMaster = getFactOrderMaster(spark, Seq("customer_id", "order_money", "create_time"))

        val dimCustomerInf = getDimCustomerInf(spark, Seq("customer_id", "customer_name"))

        val factWithDate = factOrderMaster
            .withColumn("create_date_str", substring($"create_time", 1, 8))
            .withColumn("create_date", to_date($"create_date_str", "yyyyMMdd"))
            .withColumn("year", year($"create_date"))
            .withColumn("month", month($"create_date"))
            .withColumn("day", dayofmonth($"create_date"))

        val joinedDF = factWithDate.join(dimCustomerInf, Seq("customer_id"), "inner").cache()

        val aggregatedDF = joinedDF.groupBy("customer_id", "customer_name", "year", "month", "day")
            .agg(
                count("*").cast("int").as("total_count"),
                sum("order_money").cast("double").as("total_amount")
            )
            .select("customer_id", "customer_name", "total_amount", "total_count", "year", "month", "day")

        aggregatedDF.createOrReplaceTempView("temp_user_consumption_day_aggr")

        spark.sql(
            """
              |INSERT OVERWRITE TABLE dws.user_consumption_day_aggr PARTITION (year, month, day)
              |SELECT customer_id, customer_name, total_amount, total_count, year, month, day
              |FROM temp_user_consumption_day_aggr
        """.stripMargin)

        spark.sql(
            """
              |SELECT customer_id, customer_name, total_amount, total_count, year, month, day
              |FROM dws.user_consumption_day_aggr
              |ORDER BY customer_id, total_amount DESC
              |LIMIT 5
         """.stripMargin).show(false)
    }

    def cityConsumptionDayAggr(spark: SparkSession): Unit = {
        import spark.implicits._
        setDynamicPartitionConfig(spark)

        //        val factOrderMaster = getFactOrderMaster(spark, Seq("order_money", "create_time", "city", "province"))
        //
        //        val factWithDateAndLocation = factOrderMaster
        //            .withColumn("create_date_str", substring($"create_time", 1, 8))
        //            .withColumn("create_date", to_date($"create_date_str", "yyyyMMdd"))
        //            .withColumn("year", year($"create_date"))
        //            .withColumn("month", month($"create_date"))
        //
        //        val aggregatedDF = factWithDateAndLocation.groupBy("province", "city", "year", "month")
        //            .agg(
        //                sum("order_money").cast("double").as("total_amount"),
        //                count("*").cast("int").as("total_count")
        //            )
        //            .select("province", "city", "total_amount", "total_count", "year", "month")
        //
        //        // 并按照province_name，year，month进行分组,按照total_amount逆序排序，形成sequence值
        //        val windowSpec = Window.orderBy(col("total_amount").desc)
        //        val rankedDF = aggregatedDF
        //            .withColumn("sequence", rank().over(windowSpec))
        //
        //        val renamedDF = rankedDF
        //            .withColumnRenamed("province", "city_name")
        //            .withColumnRenamed("city", "province_name")
        //
        //        renamedDF.createOrReplaceTempView("temp_city_consumption_day_aggr")

        val df = getFactOrderMaster(spark, Seq("order_money", "create_time", "city", "province"))
            .withColumn("create_date_str", substring($"create_time", 1, 8))
            .withColumn("create_date", to_date($"create_date_str", "yyyyMMdd"))
            .withColumn("year", year($"create_date"))
            .withColumn("month", month($"create_date"))
            .groupBy("city", "province", "year", "month")
            .agg(
                sum("order_money").cast("double").as("total_amount"),
                count("*").cast("int").as("total_count")
            )
            .select("province", "city", "total_amount", "total_count", "year", "month")
            // 并按照province_name，year，month进行分组,按照total_amount逆序排序，形成sequence值
            .withColumn("sequence", rank().over(Window.orderBy(($"total_amount").desc)))
            .withColumnRenamed("province", "province_name")
            .withColumnRenamed("city", "city_name")

        df.createOrReplaceTempView("temp_city_consumption_day_aggr")

        spark.sql(
            """
              |INSERT OVERWRITE TABLE dws.city_consumption_day_aggr PARTITION (year, month)
              |SELECT city_name, province_name, total_amount, total_count, sequence, year, month
              |FROM temp_city_consumption_day_aggr
            """.stripMargin)

        spark.sql(
            """
              |SELECT province_name, city_name, CAST(total_amount AS BIGINT) AS total_amount, total_count, sequence, year, month
              |FROM dws.city_consumption_day_aggr
              |ORDER BY total_count DESC, total_amount DESC
              |LIMIT 5
            """.stripMargin).show(truncate = false)
    }

    def cityAvgCmpProvince(spark: SparkSession, clickhouseUrl: String, clickhouseUser: String, clickhousePassword: String, stmt: Statement): Unit = {
        val factOrderMaster = getFactOrderMaster(spark, Seq("order_money", "create_time", "city", "province"))
        factOrderMaster.cache()
        import spark.implicits._
        val factWithDateAndLocation = factOrderMaster
            .withColumn("create_date_str", substring($"create_time", 1, 8))
            .withColumn("create_date", to_date($"create_date_str", "yyyyMMdd"))
            .withColumn("year", year($"create_date"))
            .withColumn("month", month($"create_date"))
            .withColumn("provincename", $"province")
            .withColumn("cityname", $"city")

        val cityAggregatedDF = factWithDateAndLocation.groupBy("provincename", "cityname", "year", "month")
            .agg(
                sum("order_money").cast("double").as("total_amount"),
                count("*").cast("int").as("total_count")
            )
            .select("provincename", "cityname", "total_amount", "total_count", "year", "month")

        val provinceAggregatedDF = factWithDateAndLocation.groupBy("provincename", "year", "month")
            .agg(
                sum("order_money").cast("double").as("total_amount"),
                count("*").cast("int").as("total_count")
            )
            .select("provincename", "total_amount", "total_count", "year", "month")

        val cityAvgDF = cityAggregatedDF
            .withColumn("cityavgconsumption", $"total_amount" / $"total_count")
        val provinceAvgDF = provinceAggregatedDF
            .withColumn("provinceavgconsumption", $"total_amount" / $"total_count")

        val joinedDF = cityAvgDF
            .join(provinceAvgDF, Seq("provincename", "year", "month"))
            .withColumn("comparison", when($"cityavgconsumption" > $"provinceavgconsumption", "高")
                .when($"cityavgconsumption" < $"provinceavgconsumption", "低")
                .otherwise("相同"))
            .select("cityname", "cityavgconsumption", "provincename", "provinceavgconsumption", "comparison")

        writeToClickHouse(joinedDF, "shtd_result.cityavgcmpprovince", stmt)
    }

    def cityMidCmpProvince(spark: SparkSession, clickhouseUrl: String, clickhouseUser: String, clickhousePassword: String, stmt: Statement): Unit = {
        val factOrderMaster = getFactOrderMaster(spark, Seq("order_money", "create_time", "city", "province"))

        import spark.implicits._
        val factWithDateAndLocation = factOrderMaster
            .withColumn("create_date_str", substring($"create_time", 1, 8))
            .withColumn("create_date", to_date($"create_date_str", "yyyyMMdd"))
            .withColumn("year", year($"create_date"))
            .withColumn("month", month($"create_date"))
            .withColumn("provincename", $"province")
            .withColumn("cityname", $"city")

        val cityMedianDF = factWithDateAndLocation
            .groupBy("provincename", "cityname", "year", "month")
            .agg(
                percentile_approx($"order_money", lit(0.5), lit(10000)).cast("double").as("citymidconsumption")
            )
            .select("provincename", "cityname", "citymidconsumption", "year", "month")

        val provinceMedianDF = factWithDateAndLocation
            .groupBy("provincename", "year", "month")
            .agg(
                percentile_approx($"order_money", lit(0.5), lit(10000)).cast("double").as("provincemidconsumption")
            )
            .select("provincename", "provincemidconsumption", "year", "month")

        val cityAvgDF = cityMedianDF
            .withColumn("citymidcomparison", $"citymidconsumption")
        val provinceAvgDF = provinceMedianDF
            .withColumn("provincemidcomparison", $"provincemidconsumption")

        val joinedDF = cityAvgDF
            .join(provinceAvgDF, Seq("provincename", "year", "month"))
            .withColumn("comparison",
                when($"citymidconsumption" > $"provincemidconsumption", "高")
                    .when($"citymidconsumption" < $"provincemidconsumption", "低")
                    .otherwise("相同"))
            .select("cityname", "citymidconsumption", "provincename", "provincemidconsumption", "comparison")

        writeToClickHouse(joinedDF, "shtd_result.citymidcmpprovince", stmt)
    }

    def regionTopThree(spark: SparkSession, clickhouseUrl: String, clickhouseUser: String, clickhousePassword: String, stmt: Statement): Unit = {

        val factOrderMaster = getFactOrderMaster(spark, Seq("province", "city", "order_money", "create_time"))

        import spark.implicits._

        val factWithDate = factOrderMaster
            .withColumn("create_date_str", substring($"create_time", 1, 8))
            .withColumn("create_date", to_date($"create_date_str", "yyyyMMdd"))
            .withColumn("year", year($"create_date"))
            .filter($"year" === 2022)

        val aggregatedDF = factWithDate
            .groupBy("province", "city")
            .agg(sum("order_money").cast("double").as("total_amount"))
            .orderBy($"province", $"total_amount".desc)

        val windowSpec = Window.partitionBy("province").orderBy(col("total_amount").desc)
        val rankedDF = aggregatedDF.withColumn("rank", rank().over(windowSpec)).filter($"rank" <= 3)

        val resultDF = rankedDF.groupBy("province")
            .agg(
                array_join(collect_list($"city"), ",").as("citynames"),
                array_join(collect_list(round($"total_amount").cast("bigint")), ",").as("cityamount")
            )
            .select(
                $"province".as("provincename"),
                $"citynames",
                $"cityamount"
            )

        writeToClickHouse(resultDF, "shtd_result.regiontopthree", stmt)
    }


    def topTenProducts(spark: SparkSession, clickhouseUrl: String, clickhouseUser: String, clickhousePassword: String, stmt: Statement): Unit = {
        import spark.implicits._
        val factOrderDetail = getFactOrderDetail(spark, Seq("order_detail_id", "product_id", "product_name", "product_cnt", "product_price"))

        val topQuantityDF = factOrderDetail
            .groupBy("product_id", "product_name")
            .agg(sum("product_cnt").cast("int").as("total_quantity"))
            .orderBy($"total_quantity".desc)
            .limit(10)

        val topPriceDF = factOrderDetail
            .groupBy("product_id", "product_name")
            .agg(sum(($"product_cnt" * $"product_price")).as("total_price"))
            .orderBy(($"total_price").desc)
            .limit(10)

        val rankedTopQuantity = topQuantityDF
            .withColumn("sequence", row_number().over(Window.orderBy($"total_quantity".desc)))
            .withColumnRenamed("product_id", "quantity_product_id")
            .withColumnRenamed("product_name", "quantity_product_name")

        val rankedTopPrice = topPriceDF
            .withColumn("sequence", row_number().over(Window.orderBy($"total_price".desc)))
            .withColumnRenamed("product_id", "price_product_id")
            .withColumnRenamed("product_name", "price_product_name")

        val resultDF = rankedTopQuantity.join(rankedTopPrice, Seq("sequence"), "left")
            .select(
                $"quantity_product_id".as("topquantityid"),
                $"quantity_product_name".as("topquantityname"),
                $"total_quantity".as("topquantity"),
                $"price_product_id".as("toppriceid"),
                $"price_product_name".as("toppricename"),
                $"total_price".as("topprice"),
                $"sequence"
            )

        writeToClickHouse(resultDF, "shtd_result.topten", stmt)
    }

    def userRepurchasedRate(spark: SparkSession, clickhouseUrl: String, clickhouseUser: String, clickhousePassword: String, stmt: Statement): Unit = {
        import spark.implicits._

        val factOrderMaster = getFactOrderMaster(spark, Seq("customer_id", "order_money", "create_time"))

        val factWithDate = factOrderMaster
            .withColumn("create_date_str", substring($"create_time", 1, 8))
            .withColumn("create_date", to_date($"create_date_str", "yyyyMMdd"))
            .withColumn("year", year($"create_date"))
            .withColumn("month", month($"create_date"))
            .withColumn("day", dayofmonth($"create_date"))
            .withColumn (
                "create_time",
                date_format(unix_timestamp($"create_time", "yyyyMMddHHmmss").cast("timestamp"), "yyyy-MM-dd HH:mm:ss")
            )

        val windowSpec = Window.partitionBy("customer_id").orderBy("create_date")
        val factWithLag = factWithDate
            .withColumn("prev_order_date", lag($"create_date", 1).over(windowSpec))
            .filter($"prev_order_date".isNotNull)

        // 连续两天下单的人数
        val repurchasedUserCount = factWithLag
            .filter(datediff($"create_date", $"prev_order_date") === 1)
            .select("customer_id")
            .distinct()
            .count()

        // 已下单人数
        val purchasedUserCount = factWithDate
            .select("customer_id")
            .distinct()
            .count()

        // 连续两天下单人数/已下单人数百分比（保留1位小数，四舍五入，不足的补0）例如21.1%，或者32.0%
        val repurchaseRate = f"${if (purchasedUserCount > 0) (repurchasedUserCount.toDouble / purchasedUserCount.toDouble) * 100 else 0.0}%.1f%%"

        val resultDF = Seq(
            (purchasedUserCount, repurchasedUserCount, repurchaseRate)
        ).toDF("purchaseduser", "repurchaseduser", "repurchaserate")

        writeToClickHouse(resultDF, "shtd_result.userrepurchasedrate", stmt)
    }

    def showProvinceOrderAmount(spark: SparkSession, clickhouseUrl: String, clickhouseUser: String, clickhousePassword: String, stmt: Statement): Unit = {
        val provinceOrderDF = getFactOrderMaster(spark, Seq("order_id", "province"))

        val provinceAmountDF = provinceOrderDF
            .groupBy("province")
            .agg(count("order_id").as("OrderCount"))

        // 先对长表进行排序，但此操作无效，具体原因在末尾。
        val sortedProvinceAmountDF = provinceAmountDF.orderBy(col("OrderCount").desc)

        val pivotDF = sortedProvinceAmountDF
            .groupBy()
            .pivot("province")
            .agg(first("OrderCount"))
            .na.fill(0)

        // 提取全部province列的值，转换成一个string数组
        val sortedProvinces = sortedProvinceAmountDF.select("province").collect().map(_.getString(0))

        // 将pivoted表的列顺序调整为与sortedProvinces顺序一致
        val finalDF = pivotDF.select(sortedProvinces.map(col): _*)

        finalDF.show(false)

        // 把长表排序好了后转换成宽表顺序也同样是由pivot默认排序方式决定的，因此需要重新对宽表进行操作。
    }

    def accumulateConsumption(spark: SparkSession, clickhouseUrl: String, clickhouseUser: String, clickhousePassword: String, stmt: Statement): Unit = {
        import spark.implicits._

        val factOrderMaster = getFactOrderMaster(spark, Seq("order_id", "order_money", "create_time"))

        val formattedDF = factOrderMaster
            .withColumn (
                "create_time",
                date_format(unix_timestamp($"create_time", "yyyyMMddHHmmss").cast("timestamp"), "yyyy-MM-dd HH:mm:ss")
            )

        val filterDF = formattedDF.filter(
            $"create_time" >= "2022-04-26 00:00:00" && $"create_time" <= "2022-04-26 09:59:59"
        )

        val hourlyDF = filterDF.withColumn("consumptiontime", date_format($"create_time", "yyyy-MM-dd HH"))

        val windowSpec = Window.orderBy("consumptiontime").rowsBetween(Window.unboundedPreceding, Window.currentRow)

        val resultDF = hourlyDF.groupBy("consumptiontime")
            .agg(
                sum("order_money").as("consumptionadd")  // 新增金额
            )
            .withColumn("consumptionacc", sum("consumptionadd").over(windowSpec))  // 累加金额

        writeToClickHouse(resultDF, "shtd_result.accumulateconsumption", stmt)
    }

    def slideWindowConsumption(spark: SparkSession, clickhouseUrl: String, clickhouseUser: String, clickhousePassword: String, stmt: Statement): Unit = {
        import spark.implicits._

        val factOrderMaster = getFactOrderMaster(spark, Seq("order_id", "order_money", "create_time"))

        val formattedDF = factOrderMaster
            .withColumn (
                "create_time",
                date_format(unix_timestamp($"create_time", "yyyyMMddHHmmss").cast("timestamp"), "yyyy-MM-dd HH:mm:ss")
            )

        val windowDF = formattedDF
            .withColumn("consumptiontime", window($"create_time", "5 hours", "1 hour"))

        val resultDF = windowDF.groupBy("consumptiontime")
            .agg(
                sum("order_money").as("consumptionsum"),
                count("order_id").as("consumptioncount"),
                round(sum("order_money") / count("order_id"), 2).as("consumptionavg")
            )
            .withColumn(
                "consumptiontime",
                date_format($"consumptiontime.end", "yyyy-MM-dd HH")
            )
            .filter($"consumptiontime".between("2022-04-26 04", "2022-04-26 09"))
            .orderBy("consumptiontime")
            .select("consumptiontime", "consumptionsum", "consumptioncount", "consumptionavg")

        writeToClickHouse(resultDF, "shtd_result.slidewindowconsumption", stmt)
    }

    def printTable(spark: SparkSession, clickhouseUrl: String, clickhouseUser: String, clickhousePassword: String, stmt: Statement): Unit = {
        val tables = Seq(
            "cityavgcmpprovince",
            "citymidcmpprovince",
            "regiontopthree",
            "topten",
            "userrepurchasedrate",
            "accumulateconsumption",
            "slidewindowconsumption"
        )

        tables.foreach { table =>
            val query = table match {
                case "cityavgcmpprovince" =>
                    "SELECT * FROM shtd_result.cityavgcmpprovince ORDER BY cityavgconsumption DESC, provinceavgconsumption DESC LIMIT 5"
                case "citymidcmpprovince" =>
                    "SELECT * FROM shtd_result.citymidcmpprovince ORDER BY citymidconsumption DESC, provincemidconsumption DESC LIMIT 5"
                case "regiontopthree" =>
                    "SELECT * FROM shtd_result.regiontopthree ORDER BY provincename ASC LIMIT 5"
                case "topten" =>
                    "SELECT * FROM shtd_result.topten ORDER BY sequence ASC LIMIT 5"
                case "userrepurchasedrate" =>
                    "SELECT * FROM shtd_result.userrepurchasedrate"
                case "accumulateconsumption" =>
                    "SELECT * FROM shtd_result.accumulateconsumption ORDER BY consumptiontime ASC LIMIT 5"
                case "slidewindowconsumption" =>
                    "SELECT * FROM shtd_result.slidewindowconsumption ORDER BY consumptiontime ASC LIMIT 5"
                case _ => ""
            }

            val dataFrame = spark.read
                .format("jdbc")
                .option("url", clickhouseUrl)
                .option("dbtable", s"($query) AS $table")
                .option("user", clickhouseUser)
                .option("password", clickhousePassword)
                .load()

            println(s"Table: $table")
            dataFrame.show(5, truncate = false)

            println("=" * 50)
        }
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)

        println("spark...")
        userConsumptionDayAggr(spark)
        cityConsumptionDayAggr(spark)
        // ClickHouse
        val clickhouseUrl = "jdbc:clickhouse://192.168.45.13:8123"
        val clickhouseUser = "default"
        val clickhousePassword = "123456"

        var connection: Connection = null
        var statement: Statement = null

        try {
            connection = DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword)
            statement = connection.createStatement()

            println("clickhouse...")
            cityAvgCmpProvince(spark, clickhouseUrl, clickhouseUser, clickhousePassword, statement)
            cityMidCmpProvince(spark, clickhouseUrl, clickhouseUser, clickhousePassword, statement)
            regionTopThree(spark, clickhouseUrl, clickhouseUser, clickhousePassword, statement)
            topTenProducts(spark, clickhouseUrl, clickhouseUser, clickhousePassword, statement)
            userRepurchasedRate(spark, clickhouseUrl, clickhouseUser, clickhousePassword, statement)
            showProvinceOrderAmount(spark, clickhouseUrl,clickhouseUser, clickhousePassword, statement)
            accumulateConsumption(spark, clickhouseUrl, clickhouseUser, clickhousePassword, statement)
            slideWindowConsumption(spark, clickhouseUrl, clickhouseUser, clickhousePassword, statement)

            printTable(spark, clickhouseUrl, clickhouseUser, clickhousePassword, statement)
        } catch {
            case e: Exception =>
                e.printStackTrace()
        } finally {
            if (statement != null) statement.close()
            if (connection != null) connection.close()
        }

        spark.close()
        statement.close()
        connection.close()
    }
}