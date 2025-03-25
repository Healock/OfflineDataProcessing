package ssh

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object InsertIntoExample {
    def setDynamicPartitionConfig(spark: SparkSession): Unit = {
        spark.sql("set hive.exec.dynamic.partition=true")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("set hive.exec.max.dynamic.partitions=4096")
        spark.sql("set hive.exec.max.dynamic.partitions.pernode=512")
        spark.sql("set mapreduce.job.reduces=10")
    }

    def main(args: Array[String]): Unit = {
        val serverIp="192.168.45.13"
        val config=new SparkConf()
            .setAppName("dataExtract")
            .setMaster(s"yarn")
            .set("spark.sql.warehouse.dir",s"hdfs://$serverIp:9000/user/hive/warehouse")
            .set("spark.sql.metastore.uris",s"thrift://$serverIp:9083")
            .set("spark.sql.parquet.writeLegacyFormat","true")

        val spark=SparkSession.builder().config(config).enableHiveSupport().getOrCreate()

        setDynamicPartitionConfig(spark)

        val odsTableName= "ods.customer_inf"
        val dwdTableName= "dim_customer_inf"

        spark.sql("use dwd")

        val sourceData = spark
            .table(odsTableName)

        sourceData
            .write
            .mode("append")
            .insertInto(dwdTableName)

        val result = spark.table(odsTableName)
        result.show()

        spark.stop()
    }
}