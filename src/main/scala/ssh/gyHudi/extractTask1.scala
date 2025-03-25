package ssh.gyHudi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import ssh.utils.sshUtils._

import java.util.Properties

object extractTask1 {
    def dataExtract(
                        spark: SparkSession, gyTable: String, prop: Properties, mysqlUrl: String,
                        primary: String,
                        preCombine: String,
                        partition: String): Unit = {
        val path = s"/user/hive/warehouse/hudi_gy_ods.db/$gyTable"
        val odsDF = spark.read.format("hudi").load(path)
        val sourceDF = spark.read.jdbc(mysqlUrl, gyTable, prop)
        val filterDF = if (sourceDF.columns.contains("ProducePrgCode")) sourceDF.drop("ProducePrgCode") else sourceDF
        val etlDF = filterDF.withColumn("etldate", lit(getYesterdayDate))
        writeToHudi(etlDF, odsDF, "hudi_gy_ods", gyTable, primary, preCombine, partition, "overwrite")
        spark.sql(s"msck repair table hudi_gy_ods.$gyTable")
    }

    def main(args: Array[String]): Unit = {
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)
        val (prop, mysqlUrl) = mysqlConnection("ds_db01", serverIp)
        dataExtract(spark, "ChangeRecord", prop, mysqlUrl, "ChangeID,ChangeMachineID", "ChangeEndTime", "etldate")
        dataExtract(spark, "BaseMachine", prop, mysqlUrl, "BaseMachineID", "MachineAddDate", "etldate")
        dataExtract(spark, "MachineData", prop, mysqlUrl, "MachineRecordID", "MachineRecordDate", "etldate")
        dataExtract(spark, "ProduceRecord", prop, mysqlUrl, "ProduceRecordID,ProduceMachineID", "ProduceCodeEndTime", "etldate")
    }
}
