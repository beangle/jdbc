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

package org.beangle.jdbc.engine

import org.beangle.jdbc.SqlTypes
import org.beangle.jdbc.meta.{MetadataLoadSql, SqlType}

import java.sql.Types

abstract class AbstractEngine extends Engine, AbstractDialect {

  protected[engine] var typeNames: TypeNames = _

  private val typeMappingBuilder = new TypeNames.Builder()

  val systemFunctions: SystemFunctions = new SystemFunctions

  var metadataLoadSql = new MetadataLoadSql

  var keywords: Set[String] = Set.empty[String]

  def version: Version

  def registerReserved(resourceName: String): Unit = {
    keywords ++= Engine.loadKeywords(resourceName)
  }

  override def quoteChars: (Char, Char) = {
    ('\"', '\"')
  }

  override def maxIdentifierLength: Int = {
    128
  }

  protected def registerTypes(tuples: (Int, String)*): Unit = {
    tuples foreach { tuple => typeMappingBuilder.put(tuple._1, tuple._2) }
    typeNames = typeMappingBuilder.build()
  }

  /** 按照该类型的容量进行登记
   *
   * @param tuples 类型映射
   */
  protected def registerTypes2(tuples: (Int, Int, String)*): Unit = {
    tuples foreach { tuple => typeMappingBuilder.put(tuple._1, tuple._2, tuple._3) }
    typeNames = typeMappingBuilder.build()
  }

  def toType(typeName: String): SqlType = {
    typeNames.toType(typeName)
  }

  override final def toType(sqlCode: Int): SqlType = {
    toType(sqlCode, 0, 0)
  }

  override final def toType(sqlCode: Int, precision: Int): SqlType = {
    toType(sqlCode, precision, 0)
  }

  override def toType(sqlCode: Int, precision: Int, scale: Int): SqlType = {
    val targetCode = resolveCode(sqlCode, Some(precision), None)
    if (precision == 0) {
      val p = if sqlCode == Types.BOOLEAN || sqlCode == Types.BIT then 1 else precision
      typeNames.toType(targetCode, p, scale)
    } else {
      typeNames.toType(targetCode, precision, scale)
    }
  }

  override def resolveCode(typeCode: Int, precision: Option[Int], typeName: Option[String]): Int = {
    precision match
      case None => convertType(typeCode, typeName)
      case Some(p) =>
        typeCode match {
          case Types.DECIMAL | Types.NUMERIC =>
            p match {
              case 1 => if supportBoolean then Types.BOOLEAN else typeCode
              case 5 => Types.SMALLINT
              case 10 => Types.INTEGER
              case 19 => Types.BIGINT
              case _ => typeCode
            }
          case _ => convertType(typeCode, typeName)
        }
  }

  private def convertType(typeCode: Int, typeName: Option[String]): Int = {
    typeCode match {
      case Types.BIT | Types.BOOLEAN => if supportBoolean then Types.BOOLEAN else Types.NUMERIC
      case Types.OTHER =>
        typeName match {
          case None => typeCode
          case Some(tn) => if tn.toLowerCase.contains("json") then SqlTypes.JSON else typeCode
        }
      case _ => typeCode
    }
  }

  override def supportBoolean: Boolean = true

  def functions(f: SystemFunctions => Unit): Unit = {
    f(this.systemFunctions)
  }

  override def convert(sqlType: SqlType, value: String): Option[String] = {
    if value == null then None
    else {
      if sqlType.isNumberType || sqlType.isBooleanType && !supportBoolean then
        value match
          case "true" => Some("1")
          case "false" => Some("0")
          case _ => Some(value)
      else if sqlType.isBooleanType then
        value match
          case "1" | "Y" | "true" | "yes" => Some("true")
          case "0" | "N" | "false" | "no" => Some("false")
          case _ => None
      else if sqlType.isTemporalType then Option(systemFunctions.current(sqlType))
      else Option(value)
    }
  }

  override def storeCase: StoreCase = StoreCase.Mixed

  override def catalogAsSchema: Boolean = false

  override def systemSchemas: Seq[String] = List("information_schema")
}
