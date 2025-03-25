package ssh

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object testMysql {
    def main(args: Array[String]): Unit = {
        /*
            vim /opt/module/hadoop-3.1.3/etc/hadoop/core-site.xml
            查看下面的信息，hive的数据存放在分布式文件系统（distributed file system）hdfs上
            查看hdfs运行的端口
            <property>
                <name>fs.defaultFS</name>
                <value>hdfs://bigdata1:9000</value>
            </property>
             */
        // bigdata1的IP
        val serverIp = "192.168.45.13"
        val config = new SparkConf()
        config.setAppName("testMysql")
        val spark = SparkSession.builder().config(config).getOrCreate()
        val mysqlUrl = s"jdbc:mysql://${serverIp}:3306/ds_db01?useSSL=false&tmp;useUnicode=true&tmp;charsetEncoding=utf-8"
        val prop = new Properties()
        prop.put("user", "root")
        prop.put("password", "123456")
        prop.put("driver", "com.mysql.jdbc.Driver")
        // 使用mysql url和数据库账号密码参数访问
        val mysqlDF:DataFrame = spark.read.jdbc(mysqlUrl, "customer_addr", prop)
        mysqlDF.createTempView("custom_addr")
        spark.sql("select * from custom_addr order by customer_id limit 10").show()
        spark.stop()
    }
}