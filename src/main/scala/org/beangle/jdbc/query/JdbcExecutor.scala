/*
 * Copyright (C) 2005, The Beangle Software.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.beangle.jdbc.query

import org.beangle.commons.collection.page.PageLimit
import org.beangle.commons.io.{IOs, StringBuilderWriter}
import org.beangle.commons.lang.Strings
import org.beangle.commons.logging.Logging
import org.beangle.jdbc.DefaultSqlTypeMapping
import org.beangle.jdbc.engine.{Engine, Engines}
import org.beangle.jdbc.meta.SqlType

import java.sql.{BatchUpdateException, Connection, PreparedStatement, ResultSet, SQLException, Types}
import javax.sql.DataSource
import scala.Array
import scala.collection.immutable.ArraySeq

object JdbcExecutor {
  def convert(rs: ResultSet, types: Array[Int]): Array[Any] = {
    val objs = Array.ofDim[Any](types.length)
    types.indices foreach { i =>
      objs(i) = types(i) match {
        // timestamp 在驱动中类型会和java.sql.Timestamp不同
        case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE => rs.getTimestamp(i + 1)
        case Types.DATE => rs.getDate(i + 1)
        case Types.BLOB =>
          val blob = rs.getBlob(i + 1)
          if null == blob then null else blob.getBytes(1, blob.length.toInt)
        case Types.CLOB =>
          val clob = rs.getClob(i + 1)
          if null == clob then null else clob.getSubString(1, clob.length.toInt)
        case Types.LONGVARCHAR =>
          val r = rs.getCharacterStream(i + 1)
          if null == r then null
          else
            val sw = new StringBuilderWriter(16)
            IOs.copy(r, sw)
            sw.toString
        case _ => rs.getObject(i + 1)
      }
    }
    objs
  }

  def getColumnTypes(rs: ResultSet, engine: Engine): Array[Int] = {
    val meta = rs.getMetaData
    val cols =
      if (meta.getColumnName(meta.getColumnCount) == "_rownum_") {
        meta.getColumnCount - 1
      } else {
        meta.getColumnCount
      }
    val typ = Array.ofDim[Int](cols)
    (0 until cols) foreach { i =>
      typ(i) = engine.resolveCode(meta.getColumnType(i + 1), None, Some(meta.getColumnTypeName(i + 1)))
    }
    typ
  }

  def getColumnNames(rs: ResultSet): Array[String] = {
    val meta = rs.getMetaData
    val cols =
      if (meta.getColumnName(meta.getColumnCount) == "_rownum_") {
        meta.getColumnCount - 1
      } else {
        meta.getColumnCount
      }
    val columnNames = Array.ofDim[String](cols)
    (0 until cols) foreach { i =>
      columnNames(i) = meta.getColumnName(i + 1)
    }
    columnNames
  }

  def getColumnDisplaySizes(rs: ResultSet): Array[Int] = {
    val meta = rs.getMetaData
    val cols =
      if (meta.getColumnName(meta.getColumnCount) == "_rownum_") {
        meta.getColumnCount - 1
      } else {
        meta.getColumnCount
      }
    val displaySizes = Array.ofDim[Int](cols)
    (0 until cols) foreach { i =>
      displaySizes(i) = meta.getColumnDisplaySize(i + 1)
    }
    displaySizes
  }
}

class JdbcExecutor(dataSource: DataSource) extends Logging {

  private val engine = Engines.forDataSource(dataSource)
  val sqlTypeMapping = new DefaultSqlTypeMapping(engine)
  var showSql = false
  var fetchSize = 1000

  def unique[T](sql: String, params: Any*): Option[T] = {
    val rs = query(sql, params: _*)
    if (rs.isEmpty) {
      None
    } else {
      val o = rs.head
      Some(o.head.asInstanceOf[T])
    }
  }

  def queryForInt(sql: String): Option[Int] = {
    val num: Option[Number] = unique(sql)
    num match {
      case Some(n) => Some(n.intValue)
      case None => None
    }
  }

  def queryForLong(sql: String): Option[Long] = {
    val num: Option[Number] = unique(sql)
    num match {
      case Some(n) => Some(n.longValue)
      case None => None
    }
  }

  def useConnection[T](f: Connection => T): T = {
    val conn = dataSource.getConnection()
    try {
      f(conn)
    } finally {
      if (null != conn) {
        conn.close()
      }
    }
  }

  def statement(sql: String): Statement = {
    new Statement(sql, this)
  }

  def iterate(sql: String, params: Any*): ResultSetIterator = {
    if (showSql) println("JdbcExecutor:" + sql)
    val conn = dataSource.getConnection()
    conn.setAutoCommit(false)
    val stmt = conn.prepareStatement(sql)
    stmt.setFetchSize(fetchSize)
    TypeParamSetter(sqlTypeMapping, params)(stmt)
    val rs = stmt.executeQuery()
    new ResultSetIterator(rs, engine)
  }

  def query(sql: String, params: Any*): collection.Seq[Array[Any]] = {
    query(sql, TypeParamSetter(sqlTypeMapping, params))
  }

  def query(sql: String, setter: PreparedStatement => Unit): collection.Seq[Array[Any]] = {
    if (showSql) println("JdbcExecutor:" + sql)
    useConnection { conn =>
      val stmt = conn.prepareStatement(sql)
      setter(stmt)
      new ResultSetIterator(stmt.executeQuery(), engine).listAll()
    }
  }

  def fetch(sql: String, limit: PageLimit, params: Any*): collection.Seq[Array[Any]] = {
    fetch(sql, limit, TypeParamSetter(sqlTypeMapping, params))
  }

  def fetch(sql: String, limit: PageLimit, setter: PreparedStatement => Unit): collection.Seq[Array[Any]] = {
    val rs = engine.limit(sql, limit.pageSize * (limit.pageIndex - 1), limit.pageSize)
    if (showSql) println("JdbcExecutor:" + rs._1)
    useConnection { conn =>
      val stmt = conn.prepareStatement(rs._1)
      setter(stmt)
      var start = stmt.getParameterMetaData.getParameterCount - rs._2.size
      rs._2 foreach { i =>
        stmt.setInt(start + 1, i)
        start += 1
      }
      new ResultSetIterator(stmt.executeQuery(), engine).listAll()
    }
  }

  def update(sql: String, params: Any*): Int = {
    update(sql, TypeParamSetter(sqlTypeMapping, params))
  }

  def update(sql: String, setter: PreparedStatement => Unit): Int = {
    if (showSql) println("JdbcExecutor:" + sql)
    var stmt: PreparedStatement = null
    val conn = dataSource.getConnection
    if (conn.getAutoCommit) conn.setAutoCommit(false)
    var rows = 0
    try {
      stmt = conn.prepareStatement(sql)
      setter(stmt)
      rows = stmt.executeUpdate()
      stmt.close()
      stmt = null
      conn.commit()
    } catch {
      case e: SQLException =>
        conn.rollback()
        rethrow(e, sql)
    } finally {
      if (null != stmt) stmt.close()
      conn.close()
    }
    rows
  }

  def batch(sql: String, datas: collection.Seq[Array[_]], types: collection.Seq[Int]): Seq[Int] = {
    if (showSql) println("JdbcExecutor:" + sql)
    var stmt: PreparedStatement = null
    val conn = dataSource.getConnection
    if (conn.getAutoCommit) conn.setAutoCommit(false)
    val rows = new collection.mutable.ListBuffer[Int]
    var curParam: Array[_] = null
    try {
      stmt = conn.prepareStatement(sql)
      for (param <- datas) {
        curParam = param
        ParamSetter.setParams(stmt, param, types)
        stmt.addBatch()
      }
      rows ++= stmt.executeBatch()
      conn.commit()
    } catch {
      case be: BatchUpdateException =>
        rollback(conn)
        rethrow2(if be.getNextException == null then be else be.getNextException, sql, types, curParam)
      case e: SQLException =>
        rollback(conn)
        rethrow2(e, sql, types, curParam)
    } finally {
      stmt.close()
      conn.close()
    }
    rows.toList
  }

  private def rollback(conn: Connection): Unit = {
    try {
      conn.rollback()
    } catch
      case e: Exception =>
  }

  protected def rethrow(cause: SQLException, sql: String, params: Any*): Unit = {
    val msg = new StringBuffer(if (cause.getMessage == null) "" else cause.getMessage)
    msg.append(" Query: ").append(sql).append(" Parameters: ")
    if (params == null) msg.append("[]")
    else msg.append(Strings.join(params, ","))

    val e = new SQLException(msg.toString, cause.getSQLState, cause.getErrorCode)
    e.setNextException(cause)
    throw e
  }

  protected def rethrow2(cause: SQLException, sql: String, types: collection.Seq[Int], params: Array[_]): Unit = {
    val msg = new StringBuffer(if (cause.getMessage == null) "" else cause.getMessage)
    msg.append(" Query: ").append(sql).append(" Parameters: (")
    if (params == null) msg.append("[]")
    else msg.append(Strings.join(params, ","))
    var l = 0
    params foreach {
      case null => l += 0
      case s: String => l += s.length
      case _ =>
    }
    println(s"string length is ${l}")
    msg.append(") Parameter types: (").append(Strings.join(types.map(SqlType.typeName), ",")).append(')')

    val e = new SQLException(msg.toString, cause.getSQLState, cause.getErrorCode)
    e.setNextException(cause)
    throw e
  }
}
