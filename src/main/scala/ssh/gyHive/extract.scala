package ssh.gyHive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import ssh.utils.sshUtils._

import java.util.Properties

object 
extract {
    def dataExtract(spark: SparkSession, dstable: String, mysqlUrl: String, prop: Properties): Unit ={
        val mysqlDF = spark.read.jdbc(mysqlUrl, dstable, prop)
        val filterDF = if (mysqlDF.columns.contains("ProducePrgCode")) mysqlDF.drop("ProducePrgCode") else mysqlDF
        val etlDF = filterDF.withColumn("etldate", lit(getYesterdayDate))
        writeToHive(etlDF, "overwrite", dstable, Seq("etldate"))
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)
        val (mysqlUrl, prop) = mysqlConnection("ds_db01", serverIp)
        spark.sql("use ods")
        dataExtract(spark, "ChangeRecord", prop, mysqlUrl)
        dataExtract(spark, "BaseMachine", prop, mysqlUrl)
        dataExtract(spark, "MachineData", prop, mysqlUrl)
        dataExtract(spark, "ProduceRecord", prop, mysqlUrl)
    }
}
