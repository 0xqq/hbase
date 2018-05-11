import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object CreateHBaseTable {

  private val CF = Bytes.toBytes("CF")
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("spark create hbase table").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val HConfig = HBaseConfiguration.create()
    HConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT,"2181")
    HConfig.set(HConstants.ZOOKEEPER_ZNODE_PARENT,"/hbase")
    HConfig.set(HConstants.ZOOKEEPER_QUORUM,CLUSTER_IP)

    createSchemaTable(HConfig, "TABLE")
    sc.stop()
  }

  @throws[IOException]
  private def createSchemaTable(config: Configuration, table:String) = {
    try {
      val admin = new HBaseAdmin(config)
      val newTable = new HTableDescriptor(TableName.valueOf(table))
      newTable.addFamily(new HColumnDescriptor(CF))
      createOrOverwrite(admin, newTable)
    }
  }

  @throws[IOException]
  private def createOrOverwrite(admin: HBaseAdmin, table: HTableDescriptor) = {
    if (admin.tableExists(table.getTableName)) {
      admin.disableTable(table.getTableName)
      admin.deleteTable(table.getTableName)
    }
    admin.createTable(table)
  }
}
