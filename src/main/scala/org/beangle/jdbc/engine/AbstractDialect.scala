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

import org.beangle.commons.collection.Collections
import org.beangle.commons.lang.Strings.{isEmpty, join, replace}
import org.beangle.jdbc.meta.*

trait AbstractDialect extends Dialect {
  self: Engine =>

  protected var options = new Options

  override def createSchema(name: String): String = s"create schema ${name}"

  /** Table creation sql
   */
  override def createTable(table: Table): String = {
    val buf = new StringBuilder("create table").append(' ').append(table.qualifiedName).append(" (")
    val iter = table.columns.iterator
    while (iter.hasNext) {
      val col: Column = iter.next()
      buf.append(col.name.toLiteral(this)).append(' ')
      buf.append(col.sqlType.name)

      col.defaultValue foreach { dv => buf.append(" default ").append(dv) }
      if !col.nullable then buf.append(" not null")

      if (col.hasCheck && options.table.create.supportsColumnCheck) {
        buf.append(" check (").append(col.check.get).append(")")
      }
      if (!options.comment.supportsCommentOn) {
        col.comment foreach { c => buf.append(s" comment '$c'") }
      }
      if (iter.hasNext) buf.append(", ")
    }
    buf.append(')')
    if (!options.comment.supportsCommentOn) {
      table.comment foreach { c => buf.append(s" comment '$c'") }
    }
    buf.toString
  }

  override def truncate(table: Table): String = {
    replace(options.table.truncate.sql, "{name}", table.qualifiedName)
  }

  override def alterTable(table: Table): AlterTableDialect = {
    DefaultAlterTableDialect(table, options)
  }

  /** Table removal sql
   */
  override def dropTable(table: String): String = {
    replace(options.table.drop.sql, "{name}", table)
  }

  override def commentOnColumn(table: Table, column: Column, comment: Option[String]): Option[String] = {
    if (options.comment.supportsCommentOn) {
      Some("comment on column " + table.qualifiedName + '.' + column.name.toLiteral(table.engine) + " is '" + comment.getOrElse("") + "'")
    } else {
      None
    }
  }

  override def commentOnTable(table: String, comment: Option[String]): Option[String] = {
    if (options.comment.supportsCommentOn) {
      Some("comment on table " + table + " is '" + comment.getOrElse("") + "'")
    } else {
      None
    }
  }

  override def commentsOnTable(table: Table, includeMissing: Boolean): List[String] = {
    if (options.comment.supportsCommentOn) {
      val comments = Collections.newBuffer[String]
      val tableName = table.qualifiedName
      var tableComment = table.commentAndModule.getOrElse(s"${tableName}?")
      if (includeMissing) {
        comments += ("comment on table " + tableName + " is '" + tableComment + "'")
        table.columns foreach { c =>
          comments += ("comment on column " + tableName + '.' + c.name + " is '" + c.comment.getOrElse(s"${c.name}?") + "'")
        }
      } else {
        table.comment foreach { c =>
          comments += ("comment on table " + tableName + " is '" + tableComment + "'")
        }
        table.columns foreach { c =>
          c.comment foreach { cc =>
            comments += ("comment on column " + tableName + '.' + c.name + " is '" + cc + "'")
          }
        }
      }
      comments.toList
    } else {
      List.empty
    }
  }

  override def limit(query: String, offset: Int, size: Int): (String, List[Int]) = {
    val hasOffset = offset > 0
    val limitOrMax = if (null == options.limit.offsetPattern) offset + size else size

    if (hasOffset) {
      val params = if (options.limit.bindInReverseOrder) List(limitOrMax, offset) else List(offset, limitOrMax)
      (replace(options.limit.offsetPattern, "{}", query), params)
    } else {
      (replace(options.limit.pattern, "{}", query), List(limitOrMax))
    }
  }

  override def createSequence(seq: Sequence): String = {
    if (!options.sequence.supports) return null
    var sql: String = options.sequence.createSql
    sql = sql.replace("{name}", seq.qualifiedName)
    sql = sql.replace("{start}", String.valueOf(seq.current + 1))
    sql = sql.replace("{increment}", String.valueOf(seq.increment))
    sql = sql.replace("{cache}", String.valueOf(seq.cache))
    sql = sql.replace("{cycle}", if (seq.cycle) "cycle" else "")
    sql
  }

  override def dropSequence(seq: Sequence): String = {
    if (!options.sequence.supports) return null
    options.sequence.dropSql.replace("{name}", seq.qualifiedName)
  }

  override def createIndex(i: Index): String = {
    val buf = new StringBuilder("create")
      .append(if (i.unique) " unique" else "")
      .append(" index ")
      .append(i.literalName)
      .append(" on ")
      .append(i.table.qualifiedName)
      .append(" (")
    val iter = i.columns.iterator
    while (iter.hasNext) {
      buf.append(iter.next())
      if (iter.hasNext) buf.append(", ")
    }
    buf.append(")")
    buf.toString
  }

  override def dropIndex(i: Index): String = {
    if i.table.schema.name.value.nonEmpty then
      "drop index " + i.table.schema.name.toString + "." + i.literalName
    else
      "drop index " + i.literalName
  }

  override def insert(table: Table): String = {
    val sb = new StringBuilder("insert into ")
    sb ++= table.qualifiedName
    sb += '('
    sb ++= table.quotedColumnNames.mkString(",")
    sb ++= ") values("
    sb ++= ("?," * table.columns.size)
    sb.setCharAt(sb.length() - 1, ')')
    sb.mkString
  }

  override def query(table: Relation): String = {
    val sb: StringBuilder = new StringBuilder()
    sb.append("select ")
    for (columnName <- table.quotedColumnNames) {
      sb.append(columnName).append(',')
    }
    sb.deleteCharAt(sb.length() - 1)
    sb.append(" from ").append(table.qualifiedName)
    sb.toString()
  }

  override def supportSequence: Boolean = {
    null != options.sequence
  }

  override def supportMultiValueInsert: Boolean = false
}

class DefaultAlterTableDialect(table: Table, options: Options) extends AlterTableDialect(table) {

  override def addColumn(col: Column): List[String] = {
    val buf = Collections.newBuffer[String]
    var sql = s"alter table ${table.qualifiedName} add column ${col.name} ${col.sqlType.name}"
    col.defaultValue foreach { v =>
      if (col.sqlType.isStringType) {
        sql += s" default '$v''"
      } else {
        sql += s" default $v"
      }
    }
    if (options.table.create.supportsColumnCheck) {
      col.check foreach { c =>
        sql += s" check $c"
      }
    }
    buf += sql
    if (!col.nullable) {
      buf += modifyColumnSetNotNull(col)
    }
    buf.toList
  }

  override def renameColumn(col: Column, newName: String): String = {
    var renameClause = options.table.alter.renameColumn
    renameClause = replace(renameClause, "{oldcolumn}", col.name.toLiteral(table.engine))
    renameClause = replace(renameClause, "{newcolumn}", newName)
    renameClause = replace(renameClause, "{type}", col.sqlType.name)
    renameClause = replace(renameClause, "{table}", table.qualifiedName)
    s"alter table ${table.qualifiedName} $renameClause"
  }

  override def dropColumn(col: Column): String = {
    var alterClause = options.table.alter.dropColumn
    alterClause = replace(alterClause, "{column}", col.name.toLiteral(table.engine))
    s"alter table ${table.qualifiedName} $alterClause"
  }

  override def modifyColumnSetNotNull(col: Column): String = {
    var alterClause = options.table.alter.setNotNull
    alterClause = replace(alterClause, "{column}", col.name.toLiteral(table.engine))
    alterClause = replace(alterClause, "{type}", col.sqlType.name)
    s"alter table ${table.qualifiedName} $alterClause"
  }

  override def modifyColumnDropNotNull(col: Column): String = {
    var alterClause = options.table.alter.dropNotNull
    alterClause = replace(alterClause, "{column}", col.name.toLiteral(table.engine))
    alterClause = replace(alterClause, "{type}", col.sqlType.name)
    s"alter table ${table.qualifiedName} $alterClause"
  }

  override def modifyColumnDefault(col: Column, v: Option[String]): String = {
    var alterClause = v match {
      case Some(_) => options.table.alter.setDefault
      case None => options.table.alter.dropDefault
    }
    alterClause = replace(alterClause, "{column}", col.name.toLiteral(table.engine))
    var value = v.getOrElse("null")
    if (col.sqlType.isStringType && !value.startsWith("'")) {
      value = s"'$value'"
    }
    alterClause = replace(alterClause, "{value}", value)
    s"alter table ${table.qualifiedName} $alterClause"
  }

  override def modifyColumnType(col: Column, sqlType: SqlType): String = {
    var alterClause = options.table.alter.changeType
    alterClause = replace(alterClause, "{column}", col.name.toLiteral(table.engine))
    alterClause = replace(alterClause, "{type}", sqlType.name)
    s"alter table ${table.qualifiedName} $alterClause"
  }

  override def addForeignKey(fk: ForeignKey): String = {
    require(null != fk.name && null != fk.table && null != fk.referencedTable)
    require(fk.referencedColumns.nonEmpty, " reference columns is empty.")
    require(fk.columns.nonEmpty, s"${fk.name} column's size should greate than 0")

    val engine = fk.table.engine
    val referencedColumnNames = fk.referencedColumns.map(x => x.toLiteral(engine)).toList
    val result = "alter table " + fk.table.qualifiedName + foreignKeySql(fk.literalName, fk.columnNames,
      fk.referencedTable.qualifiedName, referencedColumnNames)

    if (fk.cascadeDelete && options.constraint.supportsCascadeDelete) result + " on delete cascade" else result
  }

  override def addPrimaryKey(pk: PrimaryKey): String = {
    var alterClause = options.table.alter.addPrimaryKey
    alterClause = replace(alterClause, "{name}", pk.name.toLiteral(table.engine))
    alterClause = replace(alterClause, "{column-list}", nameList(pk.columns, table.engine))
    s"alter table ${table.qualifiedName} $alterClause"
  }

  override def dropPrimaryKey(pk: PrimaryKey): String = {
    val dk = options.table.alter.dropPrimaryKey
    if isEmpty(dk) then this.dropConstraint(pk.name.toLiteral(table.engine))
    else s"alter table ${table.qualifiedName} $dk"
  }

  override def dropConstraint(name: String): String = {
    var alterClause = options.table.alter.dropConstraint
    alterClause = replace(alterClause, "{name}", name)
    s"alter table ${table.qualifiedName} $alterClause"
  }

  override def addUnique(fk: UniqueKey): String = {
    require(null != fk.name && null != fk.table)
    require(fk.columns.nonEmpty, s"${fk.name} column's size should great than 0")
    "alter table " + fk.table.qualifiedName + " add constraint " + fk.literalName + " unique (" + nameList(fk.columns, table.engine) + ")"
  }

  protected def foreignKeySql(constraintName: String, foreignKey: Iterable[String],
                              referencedTable: String, primaryKey: Iterable[String]): String = {
    val res: StringBuffer = new StringBuffer(30)
    res.append(" add constraint ").append(constraintName).append(" foreign key (")
      .append(join(foreignKey, ", ")).append(") references ").append(referencedTable)
    if (primaryKey.nonEmpty) {
      res.append(" (").append(join(primaryKey, ", ")).append(')')
    }
    res.toString
  }

  private def nameList(seq: Iterable[Identifier], engine: Engine): String = {
    seq.map(_.toLiteral(engine)).mkString(",")
  }
}
