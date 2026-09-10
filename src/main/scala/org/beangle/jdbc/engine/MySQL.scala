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

import org.beangle.commons.lang.Strings
import org.beangle.jdbc.SqlTypes.*
import org.beangle.jdbc.meta.TableType.{InMemory, Temporary}
import org.beangle.jdbc.meta.{Column, Index, Relation, Table, TableType}

import java.sql.Types.*

class MySQL5 extends AbstractEngine {
  registerReserved("mysql.txt")

  registerTypes(
    CHAR -> "char($l)", VARCHAR -> "longtext", LONGVARCHAR -> "longtext",
    NCHAR -> "nchar($l)", NVARCHAR -> "nvarchar($l)", LONGNVARCHAR -> "nvarchar($l)",
    BOOLEAN -> "tinyint(1)", BIT -> "bit",
    TINYINT -> "tinyint", SMALLINT -> "smallint", INTEGER -> "integer", BIGINT -> "bigint",
    REAL -> "real", FLOAT -> "float", DOUBLE -> "double precision",
    DECIMAL -> "decimal($p,$s)", NUMERIC -> "decimal($p,$s)",
    DATE -> "date", TIME -> "time", TIMESTAMP -> "datetime", TIMESTAMP_WITH_TIMEZONE -> "timestamp",
    BINARY -> "binary($l)", VARBINARY -> "longblob", LONGVARBINARY -> "longblob",
    BLOB -> "longblob", CLOB -> "longtext", NCLOB -> "longtext",
    JAVA_OBJECT -> "json", JSON -> "json")

  registerTypes2(
    (VARCHAR, 500, "varchar($l)"),
    (VARCHAR, 65535, "text"),
    (VARCHAR, 16777215, "mediumtext"),

    (NUMERIC, 65, "decimal($p, $s)"),
    (NUMERIC, Int.MaxValue, "decimal(65, $s)"),

    (BINARY, 255, "binary($l)"),
    (BINARY, 65535, "blob"),
    (BINARY, Int.MaxValue, "longblob"),

    (VARBINARY, 255, "tinyblob"),
    (VARBINARY, 65535, "blob"),
    (VARBINARY, 16777215, "mediumblob"),
    (LONGVARBINARY, 16777215, "mediumblob"))

  options.sequence.supports = false
  options.table.alter { a =>
    a.addColumn = "add {column} {type}"
    a.changeType = "modify column {column} {type}"
    a.setDefault = "alter {column} set default {value}"
    a.dropDefault = "alter {column} drop default"
    a.setNotNull = "modify {column} {type} not null"
    a.dropNotNull = "modify {column} {type}"
    a.dropColumn = "drop column {column}"
    a.renameColumn = "change column {oldcolumn} {newcolumn} {type}"

    a.addPrimaryKey = "add primary key ({column-list})"
    a.dropPrimaryKey = "drop primary key"
    a.dropConstraint = "drop constraint {name}"
  }

  options.limit.pattern = "{} limit ?"
  options.limit.offsetPattern = "{} limit ? offset ?"
  options.limit.bindInReverseOrder = true

  options.comment.supportsCommentOn = false

  metadataLoadSql.basicSql = "show variables like 'character_set_database'"

  functions { f =>
    f.currentDate = "current_date"
    f.localTime = "current_time"
    f.currentTime = "current_time"
    f.localTimestamp = "current_timestamp(6)"
    f.currentTimestamp = "current_timestamp(6)"
  }

  override def maxIdentifierLength: Int = 64

  override def catalogAsSchema: Boolean = true

  override def createSchema(name: String): String = s"create database if not exists ${name} DEFAULT CHARSET utf8 COLLATE utf8_general_ci"

  override def alterTable(table: Table): AlterTableDialect = {
    new DefaultAlterTableDialect(table, options) {
      override def foreignKeySql(constraintName: String, foreignKey: Iterable[String],
                                 referencedTable: String, primaryKey: Iterable[String]): String = {
        val cols = Strings.join(foreignKey, ", ")
        new StringBuffer(30).append(" add index ").append(constraintName).append(" (").append(cols)
          .append("), add constraint ").append(constraintName).append(" foreign key (").append(cols)
          .append(") references ").append(referencedTable).append(" (")
          .append(Strings.join(primaryKey, ", ")).append(')').toString
      }
    }
  }

  override def dropIndex(i: Index): String = {
    "drop index " + i.literalName + " on " + i.table.qualifiedName
  }

  override def defaultSchema: String = "PUBLIC"

  override def name: String = "MySQL"

  override def quoteChars: (Char, Char) = ('`', '`')

  override def version: Version = Version("[5.5,)")

  override def supportMultiValueInsert: Boolean = true

  protected override def createTableOptions(tableType: TableType): (String, String) = {
    tableType match {
      case Temporary => ("temporary", "")
      case InMemory => ("", "ENGINE=MEMORY")
      case _ => super.createTableOptions(tableType)
    }
  }

  /** Called from Relation.attach after per-column mapping.
   * MySQL row size (excluding TEXT/BLOB payload) cannot exceed 65535 bytes.
   * utf8mb4 `varchar(500)` is ~2002 bytes in-row; many such columns overflow.
   *
   * @see https://dev.mysql.com/doc/refman/8.4/en/column-count-limit.html
   */
  override def adjust(relation: Relation): Unit = {
    fitVarcharRowSize(relation)
  }

  /** MySQL maximum row size, not counting TEXT/BLOB payloads.
   *
   * @see https://dev.mysql.com/doc/refman/8.4/en/column-count-limit.html
   */
  private val MaxInRowBytes = 65535

  /** Rewrite the widest in-row VARCHAR/CHAR columns to TEXT (`toType(VARCHAR, 65535)`)
   * until the estimated row size fits under [[MaxInRowBytes]].
   * Leave a 64-byte slack for null bits and other InnoDB overhead.
   */
  private def fitVarcharRowSize(relation: Relation): Unit = {
    val bpc = charsetBytes(relation)
    while (relation.columns.map(c => inRowBytes(c, bpc)).sum > MaxInRowBytes - 64) {
      val widest = relation.columns.filter(isInRowVarchar).maxByOption(_.sqlType.precision.getOrElse(0))
      widest match {
        case None => return
        case Some(col) => col.sqlType = toType(VARCHAR, 65535)
      }
    }
  }

  /** Max bytes per character for the table's database encoding.
   * Default utf8mb4 (4). utf8/utf8mb3 is 3; GBK/GB2312/Big5 is 2.
   *
   * @see https://dev.mysql.com/doc/refman/8.4/en/charset-unicode.html
   */
  private def charsetBytes(relation: Relation): Int = {
    val enc = Option(relation.schema.database.encoding).getOrElse("utf8mb4").toLowerCase
    if (enc.contains("utf8mb4") || enc.contains("utf-8")) 4
    else if (enc.contains("utf8")) 3
    else if (enc.contains("gbk") || enc.contains("gb2312") || enc.contains("big5")) 2
    else 4
  }

  /** Columns whose declared max length is stored in-row (not TEXT/BLOB).
   * These are the candidates that can be demoted when the row is too wide.
   */
  private def isInRowVarchar(col: Column): Boolean = {
    val name = col.sqlType.name.toLowerCase
    (name.startsWith("varchar") || name.startsWith("char") || name.startsWith("nvarchar")) &&
      !name.contains("text")
  }

  /** Estimated contribution of one column to the MySQL 65535-byte row limit.
   * TEXT/BLOB/JSON count as ~12 bytes (off-page pointer). VARCHAR/CHAR use
   * `precision * bytesPerChar` plus a 1-byte or 2-byte length prefix
   * (2 bytes when the byte length exceeds 255). Other types use 8 as a
   * conservative stand-in for ints/dates.
   *
   * @see https://dev.mysql.com/doc/refman/8.4/en/storage-requirements.html
   */
  private def inRowBytes(col: Column, bpc: Int): Int = {
    val name = col.sqlType.name.toLowerCase
    if (name.contains("text") || name.contains("blob") || name.contains("json")) 12
    else if (isInRowVarchar(col)) {
      val data = col.sqlType.precision.getOrElse(255) * bpc
      data + (if data > 255 then 2 else 1)
    } else 8
  }
}
