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

import org.beangle.jdbc.SqlTypes.*
import org.beangle.jdbc.meta.TableType

import java.sql.Types.*

/**
 * DuckDB engine.
 *
 * DuckDB 默认 main schema,也支持 create schema。标识符大小写不敏感,
 * 按声明的 case 存储,这里用 Mixed 与 H2/PG 区分,避免强制大小写转换。
 */
class DuckDB extends AbstractEngine {
  registerReserved("sql-reserved.txt")

  registerTypes(
    CHAR -> "char($l)", VARCHAR -> "varchar($l)", LONGVARCHAR -> "varchar",
    NCHAR -> "char($l)", NVARCHAR -> "varchar($l)", LONGNVARCHAR -> "varchar",
    BOOLEAN -> "boolean", BIT -> "boolean",
    TINYINT -> "tinyint", SMALLINT -> "smallint", INTEGER -> "integer", BIGINT -> "bigint",
    REAL -> "real", FLOAT -> "float", DOUBLE -> "double",
    DECIMAL -> "decimal($p,$s)", NUMERIC -> "decimal($p,$s)",
    DATE -> "date", TIME -> "time", TIMESTAMP -> "timestamp", TIMESTAMP_WITH_TIMEZONE -> "timestamp with time zone",
    BINARY -> "blob", VARBINARY -> "blob", LONGVARBINARY -> "blob",
    BLOB -> "blob", CLOB -> "varchar", NCLOB -> "varchar",
    JAVA_OBJECT -> "json", JSON -> "json")

  registerTypes2(
    (VARCHAR, 1_000_000_00, "varchar($l)"), (VARCHAR, Int.MaxValue, "varchar"))

  // DuckDB 暂不支持 sequence,这里关掉避免 SequenceConverter 生成无效 sql
  options.sequence.supports = false

  options.table.drop.sql = "drop table if exists {name} cascade"
  options.table.truncate.sql = "truncate {name}"

  options.limit.pattern = "{} limit ?"
  options.limit.offsetPattern = "{} limit ? offset ?"
  options.limit.bindInReverseOrder = true
  options.comment.supportsCommentOn = true

  options.table.alter { a =>
    a.changeType = "alter {column} type {type}"
    a.setDefault = "alter {column} set default {value}"
    a.dropDefault = "alter {column} drop default"
    a.setNotNull = "alter {column} set not null"
    a.dropNotNull = "alter {column} drop not null"
    a.addColumn = "add {column} {type}"
    a.dropColumn = "drop column {column}"
    a.renameColumn = "rename {column} to {newcolumn}"

    a.addPrimaryKey = "add constraint {name} primary key ({column-list})"
    a.dropConstraint = "drop constraint if exists {name} cascade"
  }

  options.validate()

  functions { f =>
    f.currentDate = "current_date"
    f.localTime = "current_time"
    f.currentTime = "current_time"
    f.localTimestamp = "current_timestamp"
    f.currentTimestamp = "current_timestamp"
  }

  override def storeCase: StoreCase = StoreCase.Mixed

  override def defaultSchema: String = "main"

  override def name: String = "DuckDB"

  override def version: Version = Version("[0,)")

  override def supportJsonType: Boolean = true

  override def mkJsonObject(s: String): Object = s

  protected override def createTableOptions(tableType: TableType): (String, String) = {
    tableType match {
      case TableType.InMemory => ("", "")
      case _ => super.createTableOptions(tableType)
    }
  }
}
