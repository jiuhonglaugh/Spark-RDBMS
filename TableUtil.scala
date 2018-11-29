package com.my.utils.db

import java.sql.Connection
import org.apache.spark.sql.types.StructType
import scala.util.Try

/**
  * @author: Mr.Zhu
  * @create: 2018-11-25 16:50
  *
  **/
object TableUtil {
  def createTable(rddSchema:StructType,conn: Connection, table: String): Boolean = {
    Try {
      val statement = StatementUtil.createTable(rddSchema,conn, table)
      try {
       statement.execute()
      } finally {
        statement.close()
      }
    }.isSuccess
  }
  def tableExists(conn: Connection, table: String): Boolean = {
    Try {
      val statement = StatementUtil.tableExists(conn, table)
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }
  def dropTable(conn: Connection, table: String):Boolean={
    Try {
      val statement = StatementUtil.dropTable(conn, table)
      try {
        statement.executeUpdate()
      } finally {
        statement.close()
      }
    }.isSuccess
  }
  def truncateTable(conn: Connection, table: String):Boolean= {
    Try {
      val statement = StatementUtil.truncateTable(conn, table)
      try {
        statement.executeUpdate()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

}
