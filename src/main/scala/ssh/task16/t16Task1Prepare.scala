package ssh.task16

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties

object t16Task1Prepare {
    def main(args: Array[String]): Unit = {
        val serverIp = "192.168.45.13"

        val config = new SparkConf()
            .setAppName("DataExtract")
            .setMaster("yarn")
            .set("spark.sql.warehouse.dir", s"hdfs://${serverIp}:9000/user/hive/warehouse")
            .set("spark.sql.metastore.uris", s"thrift://${serverIp}:9083")
            .set("spark.sql.parquet.writeLegacyFormat", "true")

        val spark = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()

        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)

        val mysqlUrl = s"jdbc:mysql://${serverIp}:3306/ds_db01?useSSL=false&useUnicode=true&characterEncoding=utf-8"
        val prop = new Properties()
        prop.put("user", "root")
        prop.put("password", "123456")
        prop.put("driver", "com.mysql.jdbc.Driver")

        def processTable(tableName: String, createTableSQL: String): Unit = {
            try {
                spark.sql("USE ods")
                spark.sql(s"DROP TABLE IF EXISTS $tableName")
                spark.sql(createTableSQL)
//
//                spark.sql(s"INSERT INTO TABLE $tableName PARTITION(etl_date='$etlDate') SELECT * FROM ${tableName}_tmp")
                println(s"Processed $tableName with etl_date=20250318")
                println("ssh:------", spark.sql(s"select count(*) from $tableName").collect()(0).get(0), "-------")
            } catch {
                case e: Exception =>
                    println(s"Error processing $tableName: ${e.getMessage}")
            }
        }

        processTable("customer_inf",
            """
              |CREATE TABLE customer_inf (
              |customer_inf_id INT,
              |customer_id INT,
              |customer_name STRING,
              |identity_card_type TINYINT,
              |identity_card_no STRING,
              |mobile_phone STRING,
              |customer_email STRING,
              |gender CHAR(1),
              |customer_point INT,
              |register_time TIMESTAMP,
              |birthday TIMESTAMP,
              |customer_level TINYINT,
              |customer_money DECIMAL(8, 2),
              |modified_time TIMESTAMP
              |)
              |PARTITIONED BY (etl_date STRING)
              |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        """.stripMargin)

        processTable("product_info",
            """
              |CREATE TABLE product_info (
              |product_id INT,
              |product_core STRING,
              |product_name STRING,
              |bar_code STRING,
              |brand_id INT,
              |one_category_id TINYINT,
              |two_category_id TINYINT,
              |three_category_id TINYINT,
              |supplier_id INT,
              |price DECIMAL(8, 2),
              |average_cost DECIMAL(18, 2),
              |publish_status TINYINT,
              |audit_status TINYINT,
              |weight FLOAT,
              |length FLOAT,
              |height FLOAT,
              |width FLOAT,
              |color_type STRING,
              |production_date TIMESTAMP,
              |shelf_life INT,
              |descript STRING,
              |indate TIMESTAMP,
              |modified_time TIMESTAMP
              |)
              |PARTITIONED BY (etl_date STRING)
              |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        """.stripMargin
        )

        processTable("order_master",
            """
              |CREATE TABLE order_master (
              |    order_id INT,
              |    order_sn STRING,
              |    customer_id INT,
              |    shipping_user STRING,
              |    province STRING,
              |    city STRING,
              |    address STRING,
              |    order_source TINYINT,
              |    payment_method TINYINT,
              |    order_money DECIMAL(8, 2),
              |    district_money DECIMAL(8, 2),
              |    shipping_money DECIMAL(8, 2),
              |    payment_money DECIMAL(8, 2),
              |    shipping_comp_name STRING,
              |    shipping_sn STRING,
              |    create_time STRING,
              |    shipping_time STRING,
              |    pay_time STRING,
              |    receive_time STRING,
              |    order_status STRING,
              |    order_point INT,
              |    invoice_title STRING,
              |    modified_time TIMESTAMP
              |)
              |PARTITIONED BY (etl_date STRING)
              |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            """.stripMargin)

        processTable("order_detail",
            """
              |CREATE TABLE order_detail (
              |    order_detail_id INT,
              |    order_sn STRING,
              |    product_id INT,
              |    product_name STRING,
              |    product_cnt INT,
              |    product_price DECIMAL(8, 2),
              |    average_cost DECIMAL(8, 2),
              |    weight FLOAT,
              |    fee_money DECIMAL(8, 2),
              |    w_id INT,
              |    create_time STRING,
              |    modified_time TIMESTAMP
              |)
              |PARTITIONED BY (etl_date STRING)
              |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            """.stripMargin)

        spark.stop()
    }
}
