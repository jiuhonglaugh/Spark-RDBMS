package com.my.utils.db

import java.sql.{Connection, PreparedStatement}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
  * @author: Mr.Zhu
  * @create: 2018-11-25 16:14
  *
  **/
object StatementUtil {
  /**
    * INSERT INTO 语句
    * @param conn
    * @param table
    * @param rddSchema
    * @return
    */
  def insertStatement(conn: Connection, table: String, rddSchema: StructType): PreparedStatement = {
    val columns = rddSchema.fields.map(_.name).mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders)"
    conn.prepareStatement(sql)
  }

  /**
    * REPLACE INTO 语句
    * @param conn
    * @param table
    * @param rddSchema
    * @return
    */
  def replaceStatement(conn: Connection, table: String, rddSchema: StructType): PreparedStatement = {
    val columns = rddSchema.fields.map(_.name).mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val sql = s"REPLACE INTO $table ($columns) VALUES ($placeholders)"
    conn.prepareStatement(sql)
  }

  /**
    * 查看表是否存在
    * @param conn
    * @param table
    * @return
    */

  def tableExists(conn: Connection, table: String):PreparedStatement={
    conn.prepareStatement(s"SELECT * FROM $table WHERE 0=1")
  }

  /**
    * 删除表
    * @param conn
    * @param table
    * @return
    */
  def dropTable(conn: Connection, table: String):PreparedStatement={
    conn.prepareStatement(s"DROP TABLE $table")
  }

  /**
    * 清空表
    * @param conn
    * @param table
    * @return
    */
  def truncateTable(conn: Connection, table: String):PreparedStatement={
    conn.prepareStatement(s"TRUNCATE TABLE $table")
  }

  /**
    * 创建表
    * @param df
    * @param conn
    * @param table
    * @return
    */
  def createTable(rddSchema:StructType,conn: Connection, table: String):PreparedStatement={
    val sb = new StringBuilder()
    rddSchema.fields foreach { field => {
      val name = field.name
      val typ: String = getType(field.dataType.typeName)
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }}
    if (sb.length < 2) {
    }
    val sql = s"CREATE TABLE $table (${sb.substring(2)})"
    conn.prepareStatement(sql)
  }

  def getType(fileType: String):String={
    fileType match {
      case "string"=> "varchar(255)"
      case "long" => "BigInt(20)"
      case "double" => "float(10,2)"
      case "integer" => "int(10)"
      case "timestamp" => "datetime"
      case _ => ""
    }
  }
}
