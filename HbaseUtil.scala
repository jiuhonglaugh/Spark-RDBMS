package com.my.utils.db

import java.sql.{Date, Timestamp}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.{HTable, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
  * @author: Mr.Zhu
  * @create: 2018-11-28 20:13
  * 
  * 封装了 spark 将生成的 hfile 写到 hdfs 后
  * 调用 hbase API 将 spark 写入 hdfs 中的 hfile 加载到 hbase 的过程
  *
  * @param tableName Hbase 中的表名
  * @param tmpPath   hdfs 临时存储 Hfile的文件夹 存在将被删掉
  */
class HbaseUtil(tableName: String, tmpPath: String) {

  private val LOGGER = LoggerFactory.getLogger(HbaseUtil.getClass)
  private val conf = HBaseConfiguration.create()
  conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

  private val hfilePath = new Path(tmpPath)
  private val table = new HTable(conf, tableName)

  private val job = Job.getInstance(conf)
  job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
  job.setMapOutputValueClass(classOf[KeyValue])

  HFileOutputFormat.configureIncrementalLoad(job, table)

  LOGGER.info("HbaseUtil 类已经初始化完毕，即将开始将数据导入Hbase")

  /**
    * 将临时目录中的hfile加载到hbase
    */
  def bulkLoader(): Unit = {
    val bulkLoader = new LoadIncrementalHFiles(conf)
    LOGGER.info(s"开始将 Spark 写入 HDFS ${this.tmpPath}路径中的 Hfile 导入 Hbase ${this.tableName} 表中")
    bulkLoader.doBulkLoad(hfilePath, table)
  }

  def getConf(): Configuration = {
    job.getConfiguration
  }
}

/**
  * 对每一行数据进行有序封装
  * Hfile 格式要求数据书序有序
  * 顺序规则为由小到大进行排序
  * 首先按照 rowKey 进行排序，其次按照 cf 排序，最后根据 filed 排序
  */

object HbaseUtil {

  private val LOGGER = LoggerFactory.getLogger(HbaseUtil.getClass)

  /**
    * 将DataFrame 转换为Hfile格式的数据
    *
    * @param df           数据集
    * @param rddSchema    数据类型
    * @param rowKeyOffset rowkey在数据集中所在的位置 rowKey不会产生字段数据
    * @param clo          字段和列族的映射，字段为key，列族为value
    * @return
    */

  def hfileKV(df: DataFrame,
              rddSchema: StructType,
              rowKeyOffset: Int,
              cloMap: HashMap[String, String]): RDD[(ImmutableBytesWritable, KeyValue)] = {

    LOGGER.info("开始执行 flatMap 操作 将 DataFrame 转换为 Hfile 格式的数据")

    df.flatMap(row => {

      val numFields = rddSchema.fields.length
      var i = 0
      var kvs = List[Tuple2[ImmutableBytesWritable, KeyValue]]()
      val rowKey = row.get(rowKeyOffset).toString
      while (i < numFields) {
        if (rowKeyOffset == i) {
          i = i + 1
        }
        val field = rddSchema.fields(i).name
        if (!row.isNullAt(i)) {
          rddSchema.fields(i).dataType match {
            case IntegerType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getInt(i).toString)
            case LongType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getLong(i).toString)
            case DoubleType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getDouble(i).toString)
            case FloatType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getFloat(i).toString)
            case ShortType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getShort(i).toString)
            case ByteType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getByte(i).toString)
            case BooleanType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getBoolean(i).toString)
            case StringType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getString(i))
            case BinaryType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getAs[Array[Byte]](i).toString)
            case TimestampType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getAs[Timestamp](i).toString)
            case DateType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getAs[Date](i).toString)
            case t: DecimalType => kvs = kvs :+ getKV(rowKey, cloMap.get(field), field, row.getDecimal(i).toString)
            case _ => throw new IllegalArgumentException(
              s"Can't translate non-null value for field $i")
          }
        } else {
          kvs :+ getKV(rowKey, cloMap.get(field), field, "\\n")
        }
        i = i + 1
      }
      kvs
    })
  }

  /**
    * 将数据封装为 Tuple2[ImmutableBytesWritable, KeyValue] 对象
    *
    * @param rowKey    行键
    * @param clo       列族
    * @param filedName 字段
    * @param data      数据
    * @return
    */

  private def getKV(rowKey: String, clo: String, filedName: String, data: String): Tuple2[ImmutableBytesWritable, KeyValue] = {
    //    val md5RowKey = DigestUtils.md5Hex(rowKey)
    (new ImmutableBytesWritable(Bytes.toBytes(rowKey)),
      new KeyValue(Bytes.toBytes(rowKey),
        Bytes.toBytes(clo),
        Bytes.toBytes(filedName),
        Bytes.toBytes(data)))
  }

  /**
    * Spark 获取 Hbase 数据 Hfile
    *
    * @param sc           SparkContext
    * @param tableName    hbase表名
    * @param cloMap       需要读取的字段和列族的映射
    * @param fields       需要读取的字段
    * @param snapshotName hbase 数据库快照名称（创建快照方式 hbase>snapshot 'tableName','snapshotName'）
    * @param snapshotPath 快照路径，不存在将创建，用户需要有权限
    * @return
    */
  def getHbaseRDD(sc: SparkContext,
                  tableName: String,
                  snapshotPath: String,
                  snapshotName: String,
                  fields: List[String],
                  cloMap: util.HashMap[String, String]): RDD[(ImmutableBytesWritable, Result)] = {
    val max_versions = 3
    val conf = HBaseConfiguration.create
    conf.set(TableInputFormat.INPUT_TABLE, tableName);
    val scan = new Scan
    val cloSet = Set[String]()
    
    fields.foreach(filed =>{
      val clo  = cloMap.get(filed)
      scan.addColumn(Bytes.toBytes(clo), Bytes.toBytes(filed))
      cloSet.add(clo)
    })
    cloSet.toList.foreach(clo =>scan.addFamily(Bytes.toBytes(clo)))
    scan.setMaxVersions(max_versions)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val job = Job.getInstance(conf)
    TableSnapshotInputFormat.setInput(job, snapshotName, new Path(snapshotPath))
    sc.newAPIHadoopRDD(job.getConfiguration, classOf[TableSnapshotInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  }

  private def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
}

