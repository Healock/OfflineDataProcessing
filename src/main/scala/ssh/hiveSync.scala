package ssh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ssh.utils.sshUtils._

import java.util.Properties

object hiveSync {
    def createTable(spark: SparkSession, mysqlUrl: String, prop: Properties): Unit = {
        val sourceDF = spark.read.jdbc(mysqlUrl, "user_info", prop).withColumn("etl_date", lit("114514"))

        val path = s"/user/hive/warehouse/test_sync_db.db/test_sync"
        val hudiOptions = Map(
            "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
            "hoodie.datasource.write.recordkey.field" -> "id",
            "hoodie.datasource.write.precombine.field" -> "create_time",
            "hoodie.datasource.write.partitionpath.field" -> "etl_date",
            "hoodie.table.name" -> "test_sync",
            "hoodie.bloom.index.parallelism" -> "200", // Bloom 索引加速
            "hoodie.write.task.parallelism" -> "200", // 增加并行度
            "hoodie.datasource.hive_sync.enable" -> "true", // 启用 Hive 同步
            "hoodie.datasource.hive_sync.database" -> "test_sync_db", // Hive 数据库名
            "hoodie.datasource.hive_sync.table" -> "test_sync", // Hive 表名
            "hoodie.datasource.hive_sync.url" -> s"jdbc:hive2://192.168.45.13:10000/test_sync_db", // Hive JDBC URL
            "hoodie.datasource.hive_sync.partition_fields" -> "etl_date", // 分区字段（如果有）
//            "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.NonPartitionedExtractor", // 分区提取器
            "path" -> path
        )

        // 注意mode为overwrite的时候不会修改表结构，只是清空表中数据重新写入
        sourceDF.write.format("org.apache.hudi")
            .options(hudiOptions)
            .mode("overwrite")
            .save()
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val (prop, mysqlUrl) = mysqlConnection("ds_pub", serverIp)


    }
}
