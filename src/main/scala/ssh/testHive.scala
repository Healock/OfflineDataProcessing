package ssh

import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, to_timestamp, udf}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import ssh.utils.sshUtils._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter


object testHive extends Serializable{

    def main(args: Array[String]): Unit = {
        val spark = sparkConnection("192.168.45.13")

        spark.sql(
            """
              |CREATE DATABASE IF NOT EXISTS ods_ds_hudi
              |LOCATION '/user/hive/warehouse/ods_ds_hudi.db';
              |""".stripMargin)

        spark.sql(
            """
              |CREATE TABLE student (
              |  student_id STRING,
              |  name STRING,
              |  age INT,
              |  enrollment_year INT,
              |  last_updated TIMESTAMP
              |)
              |USING hudi
              |OPTIONS (
              |  'primaryKey' 'student_id',
              |  'recordkey.field' 'student_id',
              |  'precombine.field' 'last_updated',
              |  'partitionpath.field' 'enrollment_year',
              |  'table.type' 'COPY_ON_WRITE'
              |)
              |LOCATION '/user/hive/warehouse/ods_ds_hudi.db/student';
              |""".stripMargin)
    }
}