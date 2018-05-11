import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


object MyUDAF extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    StructType(StructField("ap", StringType):: StructField("c", StringType)::Nil)
  }

  override def bufferSchema: StructType = {
    StructType(StructField("ap", StringType)::Nil)
  }

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)= buffer.getString(0) + "{\"ap\":\"" + input.getString(0) + "\",\"c\":" + input.getString(1) + "},"
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)= buffer1.getString(0)+ buffer2.getString(0)
  }

  override def evaluate(buffer: Row): Any = {
    val result = buffer.getString(0)
    "[" + result.substring(0, result.length - 1)+ "]"
  }
}


//使用UDAF
 sqlContext.udf.register("MyHbaseUDAF", MyBaseUDAF)
    val pTable = sqlContext.sql("select  count(*) as counts from table group by xx")
    pTable.registerTempTable("tables")
    val peInfo = sqlContext.sql("select mac,MyBaseUDAF(ap, counts) as aps from tables group by xx")
