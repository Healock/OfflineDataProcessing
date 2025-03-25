package ssh.utils

/**
 * sshUtils
 * Author: Healock
 * @since 2025.03.11
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.{Connection, DriverManager, Statement, Timestamp}
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date, Properties, UUID}

object sshUtils {
    def setWarnLog(): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)
    }

    def setDynamicPartitionConfig(spark: SparkSession): Unit = {
        // 设置动态分区配置以及性能调优
        spark.sql("set hive.exec.dynamic.partition=true")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("set hive.exec.parallel=true")
        spark.sql("set hive.exec.parallel.thread.number=16")
        spark.sql("set hive.exec.max.dynamic.partitions=4096")
        spark.sql("set hive.exec.max.dynamic.partitions.pernode=512")
        spark.sql("set mapreduce.job.reduces=16")
    }

    def sparkConnection(serverIp: String) : SparkSession = {
        // 连接spark, 返回SparkSession
        // 建议只在main方法中调用
        val sparkConf = new SparkConf()
            .setAppName("OfflineDataProcessing")
            .setMaster("yarn")
            .set("spark.sql.warehouse.dir", s"hdfs://$serverIp:9000/user/hive/warehouse")
            .set("spark.sql.metastore.uris", s"thrift://$serverIp:9083")
            .set("spark.sql.parquet.writeLegacyFormat", "true")

        val spark = SparkSession.builder.config(sparkConf)
            .config("spark.ui.enabled", "false")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("hive.exec.parallel", "true")
            .config("hive.exec.parallel.thread.number", "16")
            .config("hive.exec.max.dynamic.partitions", "4096")
            .config("hive.exec.max.dynamic.partitions.pernode", "512")
            .config("mapreduce.job.reduces", "16")
            .enableHiveSupport().getOrCreate()

        spark
    }

    def clickhouseConnection(serverIp: String): (Statement, Connection) = {
        // 返回 statement 对象，后续调用 statement 进行 clickhouse 操作
        // 建议只在main方法中调用
        val clickhouseUrl = s"jdbc:clickhouse://${serverIp}:8123"
        val clickhouseUser = "default"
        val clickhousePassword = "123456"

        var connection: Connection = null
        var statement: Statement = null

        connection = DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword)
        statement = connection.createStatement()

        (statement,connection)
    }

    def mysqlConnection(dbname: String, serverIp: String): (Properties, String) = {
        // 建议只在main方法中调用
        val mysqlUrl = s"jdbc:mysql://${serverIp}:3306/${dbname}?useSSL=false&useUnicode=true&characterEncoding=utf-8"
        val prop = new Properties()
        prop.put("user", "root")
        prop.put("password", "123456")
        prop.put("driver", "com.mysql.jdbc.Driver")

        (prop, mysqlUrl)
    }

    def getYesterdayDate: String = {
        val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        val yesterdayDate = LocalDate.now().minusDays(1).format(formatter)

        yesterdayDate
    }

    def formatTimestamp(date: String, format: String): String = {
        if (date == null || date.equals("NULL")) {
            return null
        }

        val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val timestamp = new Timestamp(formatter.parse(date).getTime)

        DateTimeFormatter
            .ofPattern(format)
            .format(timestamp.toLocalDateTime)
    }

    def writeToHive(df: DataFrame, mode: String, dbtable: String, partitionSeq: Seq[String]): Unit = {
        val finalDF = df.repartition(partitionSeq.map(col): _*)

        finalDF
            .write
            .mode(mode)
            .partitionBy(partitionSeq: _*)
            .format("parquet")
            .saveAsTable(dbtable)
    }

    def writeToMySQL(df: DataFrame, mode: String, dbtable: String, prop: Properties, mysqlUrl: String): Unit = {
        try {
            df.write
                .mode(mode)
                .jdbc(mysqlUrl, dbtable, prop)
        } catch {
            case e: Exception =>
                e.printStackTrace()
        }
    }

    def writeToClickHouse(df: DataFrame, dbtable: String, stmt: Statement): Unit = {
        // 写入ClickHouse
        val clickhouseUrl = s"jdbc:clickhouse://$serverIp:8123"
        val clickhouseUser = "default"
        val clickhousePassword = "123456"

        val clickhouseOptions = Map(
            "url" -> clickhouseUrl,
            "user" -> clickhouseUser,
            "password" -> clickhousePassword,
            "dbtable" -> dbtable
        )

        try {
            stmt.executeUpdate(s"TRUNCATE TABLE $dbtable")

            df.write
                .format("jdbc")
                .options(clickhouseOptions)
                .mode("append")
                .save()
        } catch {
            case e: Exception =>
                e.printStackTrace()
        }
    }

    def readFromHudi(spark: SparkSession, db: String, table: String): DataFrame = {
        val path = s"/user/hive/warehouse/$db.db/$table"
        val hudiDF = spark.read.format("hudi").load(path)
        val filterDF = hudiDF.select(hudiDF.columns.drop(5).map(col): _*)

        filterDF
    }

    def writeToHudi (
                    sourceDF: DataFrame,
                    targetDF: DataFrame,
                    db: String,
                    table: String,
                    primary: String = null,
                    preCombine: String = null,
                    partition: String = null,
                    mode: String
                    ): Unit = {
        val path = s"/user/hive/warehouse/$db.db/$table"
        val hudiOptions = Map(
            "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE", //
            "hoodie.datasource.write.recordkey.field" -> primary,
            "hoodie.datasource.write.precombine.field" -> preCombine,
            "hoodie.datasource.write.partitionpath.field" -> partition,
            "hoodie.table.name" -> table,
            "hoodie.datasource.hive_sync.enable" -> "true",  // 启用 Hive 同步
            "hoodie.datasource.hive_sync.database" -> db,  // Hive 数据库名
            "hoodie.datasource.hive_sync.table" -> table,  // Hive 表名
            "hoodie.datasource.hive_sync.url" -> s"jdbc:hive2://192.168.45.13:10000/$db",  // Hive JDBC URL
            "hoodie.datasource.hive_sync.partition_fields" -> partition,  // 分区字段（如果有）
//            "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.NonPartitionedExtractor",  // 分区提取器
            "path" -> path
        )
        // 注意hudi表读入字段时会包含系统自带字段，因此需要过滤掉最开始的前5列的字段名（3月25更新）
        val saveDF = sourceDF.select(targetDF.columns.drop(5).map(col): _*)

        // 注意mode为overwrite的时候不会修改表结构，只是清空表中数据重新写入
        saveDF.write.format("org.apache.hudi")
            .options(hudiOptions)
            .mode(mode)
            .save()
    }

    def stopAll(spark: SparkSession, statement: Statement, connection: Connection, prop: Properties) : Unit = {
        // Spark
        if (spark != null) {
            spark.close()
        }
        // Clickhouse
        if (statement != null) {
            statement.close()
            connection.close()
        }
        // MySQL
        if (prop != null) {
            prop.clear()
        }
    }

    def main(args: Array[String]): Unit = {
        println("test utils")
    }

    val serverIp = "192.168.45.13"
    val yesterday = "20250323"
}
