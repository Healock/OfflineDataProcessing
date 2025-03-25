package ssh.tz

import org.apache.spark.sql.functions._
import ssh.utils.sshUtils._

object tz1 {
    def main(args: Array[String]): Unit = {
        setWarnLog()
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)

        val (prop, mysqlUrl) = mysqlConnection("ds_db01", serverIp)

        val customer_inf = spark.read.jdbc(mysqlUrl, "customer_inf", prop)
        val product_info = spark.read.jdbc(mysqlUrl, "product_info", prop)
        val order_detail = spark.read.jdbc(mysqlUrl, "order_detail", prop)
        val order_master = spark.read.jdbc(mysqlUrl, "order_master", prop)

        val filterOM = order_master
            .join(customer_inf, Seq("customer_id"), "inner")

        val filterOD = order_detail
            .join(product_info, Seq("product_id"), "inner")

        filterOM.show()
        filterOD.show()

        /*
        根据Hive的dwd库中相关表或MySQL中ds_db01中相关表，
        计算出与用户customer_id为9253的用户所购买相同商品种类最多的前10位用户
        （只考虑他俩购买过多少种相同的商品，不考虑相同的商品买了多少次），
        将10位用户customer_id进行输出，输出格式如下；
        结果格式如下：
        -------------------相同种类前10的id结果展示为：--------------------
        1,2,901,4,5,21,32,91,14,52
        */
        val orderAllDF = filterOM
            .join(filterOD, Seq("order_sn"), "inner")
            .select(
                filterOM("customer_id"),
                filterOD("product_id")
            )
            .distinct()
        orderAllDF.show()

        val user9253DF = orderAllDF
            .filter(col("customer_id") === 9253)
            .select("product_id")
            .distinct()

        println("customer_id 为 9253 购买过的商品 ID：")
        user9253DF.show()

        val productIDSDF = user9253DF.select("product_id")
        val customerBuyDF = orderAllDF
            .join(productIDSDF, Seq("product_id"))
            .groupBy("customer_id")
            .agg(count("product_id").alias("num_products"))

        println("统计每个用户购买的商品有哪些，然后计算其中 属于用户9253买过的商品 的数量")
        customerBuyDF.show()

        val finalDF = customerBuyDF
            .select(
                "customer_id",
                "num_products"
            )
            .orderBy(desc("num_products"))
            .limit(10)

        println("对上面的结果按照num_products降序排序，去掉9253本人，取前十个并且要求输出在一行中")
        finalDF.show()
    }
}
