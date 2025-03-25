package ssh.dsHive

import ssh.utils.sshUtils._

object extractPrepare {
    def main(args: Array[String]): Unit = {
        val spark = sparkConnection(serverIp)
        setWarnLog()
        val (prop, mysqlUrl) = mysqlConnection("ds_pub", serverIp)

        def processTable(tableName: String, createTableSQL: String): Unit = {
                spark.sql("USE ods")
                spark.sql(s"DROP TABLE IF EXISTS $tableName")
                spark.sql(createTableSQL)
                println(s"Processed $tableName with etl_date=$yesterday")
                println("ssh:------", spark.sql(s"select count(*) from $tableName").collect()(0).get(0), "-------")
        }

        processTable("user_info",
            """
              |CREATE TABLE user_info (
              |    id BIGINT,
              |    login_name STRING,
              |    nick_name STRING,
              |    passwd STRING,
              |    name STRING,
              |    phone_num STRING,
              |    email STRING,
              |    head_img STRING,
              |    user_level STRING,
              |    birthday DATE,
              |    gender STRING,
              |    create_time TIMESTAMP,
              |    operate_time TIMESTAMP
              |)
              |PARTITIONED BY (etl_date STRING)
  """.stripMargin)

        processTable("sku_info",
            """
              |CREATE TABLE sku_info (
              |    id BIGINT,
              |    spu_id BIGINT,
              |    price DECIMAL(10,0),
              |    sku_name STRING,
              |    sku_desc STRING,
              |    weight DECIMAL(10,2),
              |    tm_id BIGINT,
              |    category3_id BIGINT,
              |    sku_default_img STRING,
              |    create_time TIMESTAMP
              |)
              |PARTITIONED BY (etl_date STRING)
  """.stripMargin)


        processTable("base_province",
            """
              |CREATE TABLE base_province (
              |    id BIGINT,
              |    name STRING,
              |    region_id STRING,
              |    area_code STRING,
              |    iso_code STRING,
              |    create_time TIMESTAMP
              |)
              |PARTITIONED BY (etl_date STRING)
  """.stripMargin)

        processTable("base_region",
            """
              |CREATE TABLE base_region (
              |    id STRING,
              |    region_name STRING,
              |    create_time TIMESTAMP
              |)
              |PARTITIONED BY (etl_date STRING)
  """.stripMargin)

        processTable("order_info",
            """
              |CREATE TABLE order_info (
              |    id BIGINT,
              |    consignee STRING,
              |    consignee_tel STRING,
              |    final_total_amount DECIMAL(16,2),
              |    order_status STRING,
              |    user_id BIGINT,
              |    delivery_address STRING,
              |    order_comment STRING,
              |    out_trade_no STRING,
              |    trade_body STRING,
              |    create_time TIMESTAMP,
              |    operate_time TIMESTAMP,
              |    expire_time TIMESTAMP,
              |    tracking_no STRING,
              |    parent_order_id BIGINT,
              |    img_url STRING,
              |    province_id INT,
              |    benefit_reduce_amount DECIMAL(16,2),
              |    original_total_amount DECIMAL(16,2),
              |    feight_fee DECIMAL(16,2)
              |)
              |PARTITIONED BY (etl_date STRING)
  """.stripMargin)

        processTable("order_detail",
            """
              |CREATE TABLE order_detail (
              |    id BIGINT,
              |    order_id BIGINT,
              |    sku_id BIGINT,
              |    sku_name STRING,
              |    img_url STRING,
              |    order_price DECIMAL(10,2),
              |    sku_num STRING,
              |    create_time TIMESTAMP,
              |    source_type STRING,
              |    source_id BIGINT
              |)
              |PARTITIONED BY (etl_date STRING)
  """.stripMargin)

        spark.stop()
    }
}
