def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("transfer macRecorder")
    val sc = new SparkContext(sparkConf)
    val HConfig = HBaseConfiguration.create()
    HConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT,"2181")
    HConfig.set(HConstants.ZOOKEEPER_ZNODE_PARENT,"/hbase")
    HConfig.set(HConstants.ZOOKEEPER_QUORUM, CLUSTER_IP)

    val putConfig = Job.getInstance(HConfig)
    putConfig.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, DAILY_RECORDER)
    putConfig.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val scan = new Scan()
    scan.setTimeRange(startTime, endTime)
    val scanStr = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    HConfig.set(TableInputFormat.SCAN, scanStr)
    HConfig.set(TableInputFormat.INPUT_TABLE, SOURCE_TABLE)
	//read data from hbase
    val recorderRDD = sc.newAPIHadoopRDD(HConfig, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map( _._2)
    val putRecoder = recorderRDD.map(r => {
      val rowkeyBytes = r.getRow
	  val cell = r.getValue(CF, QUALI)
     
	 
      val put = new Put(rowkeyBytes)
      put.add(CF, Bytes.toBytes("CF"), Bytes.toBytes("VALUE"))

      (new ImmutableBytesWritable(),put)
    })

    putRecoder.cache()

	// write data to hbase
    dailyRecoder.saveAsNewAPIHadoopDataset(putConfig.getConfiguration)
    sc.stop()
  }
