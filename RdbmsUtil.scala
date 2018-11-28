package com.bessky.util.db

import java.sql.{Connection, PreparedStatement, SQLException}
import java.util.Properties

import com.bessky.SaveMode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/**
  * @author: Mr.Zhu
  * @create: 2018-11-25 13:18
  *
  **/
class RdbmsUtil(mode:SaveMode,create:Boolean = false) {

  private val LOGGER = LoggerFactory.getLogger("RdbmsUtil")

  /**
    * 在这里对表做创建、删除、清空、等操作
    *
    * @param df         数据集
    * @param url        数据库url
    * @param table      表名
    * @param properties 数据库连接属性
    */
  def saveTable(df: DataFrame,
                url: String,
                table: String,
                props: Properties): Unit = {
    val conn = JdbcUtils.createConnectionFactory(url, props)()
    try {
      var tableExists = TableUtil.tableExists(conn, table)

      if(!tableExists && !create){
        throw new SQLException(s"$table Table does not exist")
      }

      if (mode == SaveMode.Overwrite) {
        val sql = TableUtil.truncateTable(conn, table)
      }

      if (!tableExists && create) {
        if(!TableUtil.createTable(df.schema,conn,table)){
          throw new SQLException(s"$table Table Creation Failed")
        }
      }
    } finally {
      conn.close()
    }

    val rddSchema = df.schema
    val getConnection: () => Connection = JdbcUtils.createConnectionFactory(url, props)
    df.foreachPartition { iterator =>
      savePartition(getConnection, table, iterator, rddSchema, mode, 1000)
    }
  }

  def savePartition(getConnection: () => Connection,
                    table: String,
                    iterator: Iterator[Row],
                    rddSchema: StructType,
                    mode: SaveMode,
                    batchSize: Int = 1000): Unit = {
    val conn = getConnection()
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        LOGGER.info("Exception while detecting transaction support", e)
        true
    }
    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      var stmt: PreparedStatement = null
      if (mode == SaveMode.Replace) {
        stmt = StatementUtil.replaceStatement(conn, table, rddSchema)
      } else {
        stmt = StatementUtil.insertStatement(conn, table, rddSchema)
      }
      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = rddSchema.fields.length
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, 0)
            } else {
              rddSchema.fields(i).dataType match {
                case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                case LongType => stmt.setLong(i + 1, row.getLong(i))
                case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                case ShortType => stmt.setInt(i + 1, row.getShort(i))
                case ByteType => stmt.setInt(i + 1, row.getByte(i))
                case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                case StringType => stmt.setString(i + 1, row.getString(i))
                case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
                case _ => throw new IllegalArgumentException(
                  s"Can't translate non-null value for field $i")
              }
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } finally {
      if (!committed) {
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        try {
          conn.close()
        } catch {
          case e: Exception => LOGGER.warn("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }
}
