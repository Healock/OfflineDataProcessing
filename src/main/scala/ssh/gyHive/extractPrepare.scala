package ssh.gyHive

import ssh.utils.sshUtils._

object extractPrepare {
    def main(args: Array[String]): Unit = {
        val serverIp = "192.168.45.13"
        val spark = sparkConnection(serverIp)
        setWarnLog()

        def processTable(tableName: String, createTableSQL: String): Unit = {
            try {
                spark.sql("USE ods")
                spark.sql(s"DROP TABLE IF EXISTS $tableName")
                spark.sql(createTableSQL)

                println(s"Processed $tableName with etldate=$getYesterdayDate")
                println("ssh:------", spark.sql(s"select count(*) from $tableName").collect()(0).get(0), "-------")
            } catch {
                case e: Exception =>
                    println(s"Error processing $tableName: ${e.getMessage}")
            }
        }

        processTable("basemachine",
            """
                |CREATE TABLE basemachine (
                |BaseMachineID INT,
                |MachineFactory INT,
                |MachineNo STRING,
                |MachineName STRING,
                |MachineIP STRING,
                |MachinePort INT,
                |MachineAddDate DATE,
                |MachineRemarks STRING,
                |MachineAddEmpID INT,
                |MachineResponsEmpID INT,
                |MachineLedgerXml STRING,
                |ISWS INT
                |)
                |PARTITIONED BY (etldate STRING)
                |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            """.stripMargin)

        processTable("changerecord",
            """
                |CREATE TABLE changerecord (
                |ChangeID INT,
                |ChangeMachineID INT,
                |ChangeMachineRecordID INT,
                |ChangeRecordState STRING,
                |ChangeStartTime TIMESTAMP,
                |ChangeEndTime TIMESTAMP,
                |ChangeRecordData STRING,
                |ChangeHandleState INT
                |)
                |PARTITIONED BY (etldate STRING)
                |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            """.stripMargin)

        processTable("machinedata",
            """
                |CREATE TABLE machinedata (
                |MachineRecordID INT,
                |MachineID INT,
                |MachineRecordState STRING,
                |MachineRecordData STRING,
                |MachineRecordDate TIMESTAMP
                |)
                |PARTITIONED BY (etldate STRING)
                |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            """.stripMargin)

        processTable("producerecord",
            """
                |CREATE TABLE producerecord (
                |ProduceRecordID INT,
                |ProduceMachineID INT,
                |ProduceCodeNumber STRING,
                |ProduceStartWaitTime TIMESTAMP,
                |ProduceCodeStartTime TIMESTAMP,
                |ProduceCodeEndTime TIMESTAMP,
                |ProduceCodeCycleTime INT,
                |ProduceEndTime TIMESTAMP,
                |ProducePrgCode STRING,
                |ProduceTotalOut INT,
                |ProduceInspect INT
                |)
                |PARTITIONED BY (etldate STRING)
                |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            """.stripMargin)

        spark.stop()
    }
}
