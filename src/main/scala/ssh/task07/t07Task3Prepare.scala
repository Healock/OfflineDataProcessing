package ssh.task07

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import ssh.utils.sshUtils._

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

object t07Task3Prepare {
    def createDatabase(stmt: Statement, spark: SparkSession): Unit = {
        stmt.executeUpdate("DROP DATABASE IF EXISTS shtd_result")
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS shtd_result")
        spark.sql("DROP DATABASE IF EXISTS dws CASCADE")
        spark.sql("CREATE DATABASE IF NOT EXISTS dws")
        println("shtd_result dws database created.")
    }

    def createProvinceConsumptionDayAggr(spark: SparkSession): Unit =
        spark.sql(
            """
            |CREATE TABLE IF NOT EXISTS dwd.province_order_summary (
            |    province_id INT,
            |    province_name STRING,
            |    region_id INT,
            |    region_name STRING,
            |    total_amount DOUBLE,
            |    total_count INT,
            |    sequence INT
            |)
            |PARTITIONED BY (
            |    year INT,
            |    month INT
            |)
            """.stripMargin)

    def createUserRepurchasedRateTable(stmt: Statement): Unit = {
        stmt.executeUpdate(
            """
              |CREATE TABLE IF NOT EXISTS shtd_result.userrepurchasedrate (
              |  purchaseduser INT NOT NULL,
              |  repurchaseduser INT NOT NULL,
              |  repurchaserate DECIMAL(10,4) NOT NULL,
              |  PRIMARY KEY (purchaseduser)
              |);
        """.stripMargin
        )
        println("userrepurchasedrate table created.")
    }

    def main(args: Array[String]): Unit = {
        val objectName = this.getClass.getSimpleName
        println(s"$objectName...")
        val serverIp = "192.168.45.13"

        val (prop, mysqlUrl) = mysqlConnection("shtd_result", serverIp)

        val conn: Connection = DriverManager.getConnection(mysqlUrl, prop.getProperty("user"), prop.getProperty("password"))

        val stmt: Statement = conn.createStatement()
        val spark = sparkConnection(serverIp)

        createDatabase(stmt, spark)
        createProvinceConsumptionDayAggr(spark)
        createUserRepurchasedRateTable(stmt)
    }
}
