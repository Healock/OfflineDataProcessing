package ssh.Prepare

import ssh.utils.sshUtils._

import java.sql.{DriverManager, Statement}

object task3Prepare {
    def main(args: Array[String]): Unit = {
        setWarnLog()
        val spark = sparkConnection(serverIp)

        // 初始化 MySQL 连接
        val (mysqlProps, mysqlUrl) = mysqlConnection("", serverIp)
        val mysqlConn = DriverManager.getConnection(mysqlUrl, mysqlProps)
        val mysqlStmt = mysqlConn.createStatement()

        // 初始化 ClickHouse 连接
        val (clickhouseStmt, clickhouseConn) = clickhouseConnection(serverIp)

        try {
            mysqlStmt.executeUpdate("DROP DATABASE IF EXISTS shtd_result")
            mysqlStmt.executeUpdate("CREATE DATABASE shtd_result")

            clickhouseStmt.executeUpdate("DROP DATABASE IF EXISTS shtd_result")
            clickhouseStmt.executeUpdate("CREATE DATABASE shtd_result")

            spark.sql("DROP DATABASE IF EXISTS dws CASCADE")
            spark.sql("CREATE DATABASE dws")

            println("所有数据库已成功重建！")

            createHiveTable(spark, "province_consumption_day_aggr",
                "province_id INT, province_name STRING, region_id INT, region_name STRING, total_amount DOUBLE, total_count INT, sequence INT",
                "year INT, month INT")

            createMysqlTable(mysqlStmt, "provinceavgcmp",
                "provinceid INT, provincename TEXT, provinceavgconsumption DOUBLE, allprovinceavgconsumption DOUBLE, comparison TEXT")

            createMysqlTable(mysqlStmt, "provinceavgcmpregion",
                "provinceid INT, provincename TEXT, provinceavgconsumption DOUBLE, regionid INT, regionname TEXT, regionavgconsumption DOUBLE, comparison TEXT")

            createMysqlTable(mysqlStmt, "usercontinueorder",
                "userid INT, username TEXT, day TEXT, totalconsumption DOUBLE, totalorder INT")

            createClickhouseTable(clickhouseStmt, "provinceavgcmpregion",
                "provinceid Int32, provincename String, provinceavgconsumption Double, region_id Int32, region_name String, regionavgconsumption Double, comparison String",
                "provinceid")

            createMysqlTable(mysqlStmt, "regiontopthree",
                "regionid INT, regionname TEXT, provinceids TEXT, provincenames TEXT, provinceamount TEXT")

            createClickhouseTable(clickhouseStmt, "topten",
                "topquantityid Int32, topquantityname String, topquantity Int32, toppriceid String, toppricename String, topprice Decimal(18,2), sequence Int32",
                "sequence")

            createClickhouseTable(clickhouseStmt, "nationmedian",
                "provinceid Int32, provincename String, regionid Int32, regionname String, provincemedian Double, regionmedian Double",
                "provinceid")

            createMysqlTable(mysqlStmt, "userrepurchasedrate",
                "purchaseduser INT, repurchaseduser INT, repurchaserate TEXT")

            createClickhouseTable(clickhouseStmt, "userrepurchasedrate",
                "purchaseduser Int32, repurchaseduser Int32, repurchaserate String",
                "purchaseduser")

            createMysqlTable(mysqlStmt, "order_final_money_amount_diff",
                "order_id INT, diff_value DOUBLE")

            createMysqlTable(mysqlStmt, "max_avg_order_price_seq",
                "seq_index INT, avg_order_price DOUBLE, id_range TEXT, matchnum INT")

        } finally {
            mysqlStmt.close()
            mysqlConn.close()
            clickhouseStmt.close()
            clickhouseConn.close()
            spark.stop()
        }
    }

    // MySQL 建表方法
    def createMysqlTable(stmt: Statement, tableName: String, columns: String, extra: String = ""): Unit = {
        val sql = s"CREATE TABLE IF NOT EXISTS shtd_result.$tableName ($columns) $extra"
        stmt.executeUpdate(sql)
        println(s"MySQL 表 $tableName 创建成功")
    }

    // ClickHouse 建表方法
    def createClickhouseTable(stmt: Statement, tableName: String, columns: String, orderBy: String): Unit = {
        val sql =
            s"""
               |CREATE TABLE IF NOT EXISTS shtd_result.$tableName ($columns)
               |ENGINE = MergeTree()
               |ORDER BY $orderBy
            """.stripMargin
        stmt.executeUpdate(sql)
        println(s"ClickHouse 表 $tableName 创建成功")
    }

    // Hive 建表方法
    def createHiveTable(spark: org.apache.spark.sql.SparkSession, tableName: String, columns: String, partitionedBy: String = "", storedAs: String = "PARQUET"): Unit = {
        val partitionSql = if (partitionedBy.nonEmpty) s"PARTITIONED BY ($partitionedBy)" else ""
        val sql =
            s"""
               |CREATE TABLE IF NOT EXISTS dws.$tableName ($columns)
               |$partitionSql
               |STORED AS $storedAs
            """.stripMargin
        spark.sql(sql)
        println(s"Hive 表 $tableName 创建成功")
    }
}
