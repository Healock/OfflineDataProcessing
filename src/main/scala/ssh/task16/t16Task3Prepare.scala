package ssh.task16

import org.apache.log4j.{Level, Logger}

import java.sql.{Connection, DriverManager, Statement}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import ssh.utils.sshUtils._

object t16Task3Prepare {

    def createDwsDatabase(spark: SparkSession): Unit = {
        spark.sql("DROP DATABASE IF EXISTS dws CASCADE")
        spark.sql("CREATE DATABASE IF NOT EXISTS dws")
        println("dws database created.")
    }

    def createShtdResultDatabase(stmt: Statement): Unit = {
        stmt.executeUpdate("DROP DATABASE IF EXISTS shtd_result")
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS shtd_result")
        println("shtd_result database created.")
    }

    def createUserConsumptionDayAggrTable(spark: SparkSession): Unit = {
        try {
            spark.sql(
                """
                |CREATE TABLE IF NOT EXISTS dws.user_consumption_day_aggr (
                |  customer_id INT,
                |  customer_name STRING,
                |  total_amount DOUBLE,
                |  total_count INT
                |)
                |PARTITIONED BY (year INT, month INT, day INT)
                |STORED AS PARQUET
            """.stripMargin
            )
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            println("user_consumption_day_aggr table created.")
        }
    }

    def createCityConsumptionDayAggrTable(spark: SparkSession): Unit = {
        try {
            spark.sql(
                """
                |CREATE TABLE IF NOT EXISTS dws.city_consumption_day_aggr (
                |  city_name STRING,
                |  province_name STRING,
                |  total_amount DOUBLE,
                |  total_count INT,
                |  sequence INT
                |)
                |PARTITIONED BY (year INT, month INT)
                |STORED AS PARQUET
                """.stripMargin
            )
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            println("city_consumption_day_aggr table created.")
        }
    }

    def createCityAvgCmpProvinceTable(stmt: Statement): Unit = {
        stmt.executeUpdate(
            """
              |CREATE TABLE IF NOT EXISTS shtd_result.cityavgcmpprovince (
              |  cityname String,
              |  cityavgconsumption Float64,
              |  provincename String,
              |  provinceavgconsumption Float64,
              |  comparison String
              |) ENGINE = MergeTree()
              |ORDER BY cityname
      """.stripMargin
        )
        println("cityavgcmpprovince table created.")
    }

    def createCityMidCmpProvinceTable(stmt: Statement): Unit = {
        stmt.executeUpdate(
            """
              |CREATE TABLE IF NOT EXISTS shtd_result.citymidcmpprovince (
              |  cityname String,
              |  citymidconsumption Float64,
              |  provincename String,
              |  provincemidconsumption Float64,
              |  comparison String
              |) ENGINE = MergeTree()
              |ORDER BY cityname
      """.stripMargin
        )
        println("citymidcmpprovince table created.")
    }

    def createRegionTopThreeTable(stmt: Statement): Unit = {
        stmt.executeUpdate(
            """
              |CREATE TABLE IF NOT EXISTS shtd_result.regiontopthree (
              |  provincename String,
              |  citynames String,
              |  cityamount String
              |) ENGINE = MergeTree()
              |ORDER BY provincename
      """.stripMargin
        )
        println("regiontopthree table created.")
    }

    def createTopTenTable(stmt: Statement): Unit = {
        stmt.executeUpdate(
            """
              |CREATE TABLE IF NOT EXISTS shtd_result.topten (
              |  topquantityid Int32,
              |  topquantityname String,
              |  topquantity Int32,
              |  toppriceid String,
              |  toppricename String,
              |  topprice Decimal(10, 2),
              |  sequence Int32
              |) ENGINE = MergeTree()
              |ORDER BY topquantityid
      """.stripMargin
        )
        println("topten table created.")
    }

    def createUserRepurchasedRateTable(stmt: Statement): Unit = {
        stmt.executeUpdate(
            """
              |CREATE TABLE IF NOT EXISTS shtd_result.userrepurchasedrate (
              |  purchaseduser Int32,
              |  repurchaseduser Int32,
              |  repurchaserate String
              |) ENGINE = MergeTree()
              |ORDER BY purchaseduser
      """.stripMargin
        )
        println("userrepurchasedrate table created.")
    }

    def createAccumulateConsumptionTable(stmt: Statement): Unit = {
        stmt.executeUpdate(
            """
              |CREATE TABLE IF NOT EXISTS shtd_result.accumulateconsumption (
              |  consumptiontime String,
              |  consumptionadd Float64,
              |  consumptionacc Float64
              |) ENGINE = MergeTree()
              |ORDER BY consumptiontime
      """.stripMargin
        )
        println("accumulateconsumption table created.")
    }

    def createSlideWindowConsumptionTable(stmt: Statement): Unit = {
        stmt.executeUpdate(
            """
              |CREATE TABLE IF NOT EXISTS shtd_result.slidewindowconsumption (
              |  consumptiontime String,
              |  consumptionsum Float64,
              |  consumptioncount Float64,
              |  consumptionavg Float64
              |) ENGINE = MergeTree()
              |ORDER BY consumptiontime
      """.stripMargin
        )
        println("slidewindowconsumption table created.")
    }

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)

        println("Task3Prepare...")
        // Spark
        val serverIp = "192.168.45.13"
        val sparkConf = new SparkConf()
            .setAppName("task3Prepare")
            .setMaster("yarn")
            .set("spark.sql.warehouse.dir", s"hdfs://$serverIp:9000/user/hive/warehouse")
            .set("spark.sql.metastore.uris", s"thrift://$serverIp:9083")
            .set("spark.sql.parquet.writeLegacyFormat", "true")

        val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        createDwsDatabase(spark)
        spark.sql("USE dws")
        createUserConsumptionDayAggrTable(spark)
        createCityConsumptionDayAggrTable(spark)

        // ClickHouse
        val clickhouseUrl = "jdbc:clickhouse://192.168.45.13:8123"
        val clickhouseUser = "default"
        val clickhousePassword = "123456"

        var connection: Connection = null
        var statement: Statement = null

        try {
            connection = DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword)
            statement = connection.createStatement()

            // shtd_result
            createShtdResultDatabase(statement)

            createCityAvgCmpProvinceTable(statement)
            createCityMidCmpProvinceTable(statement)
            createRegionTopThreeTable(statement)
            createTopTenTable(statement)
            createUserRepurchasedRateTable(statement)
            createAccumulateConsumptionTable(statement)
            createSlideWindowConsumptionTable(statement)

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
