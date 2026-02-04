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

package org.beangle.jdbc.meta

import org.beangle.commons.lang.Strings
import org.beangle.commons.lang.Strings.split
import org.beangle.commons.xml.{Document, Element}
import org.beangle.jdbc.engine.Engines

import scala.language.implicitConversions

object Serializer {

  def fromXml(content: String): Database = {
    val root = Document.parse(content)
    val engine = Engines.forName(root("engine"))
    val database = new Database(engine)
    database.version = root("version")
    (root \\ "schema") foreach { schemaElem =>
      val schema = database.getOrCreateSchema(schemaElem("name"))
      (schemaElem \ "tables" \ "table") foreach { tableElem =>
        val table = schema.createTable(tableElem("name"))
        tableElem.get("comment").foreach(n => table.updateCommentAndModule(n))
        (tableElem \ "columns" \ "column") foreach { colElem =>
          val col = table.createColumn(colElem("name"), colElem("type"))
          colElem.get("nullable").foreach(n => col.nullable = n.toBoolean)
          colElem.get("unique").foreach(n => col.unique = n.toBoolean)
          colElem.get("check").foreach(n => col.check = Some(n))
          colElem.get("comment").foreach(n => col.comment = Some(n))
          colElem.get("defaultValue").foreach(n => col.defaultValue = Some(n))
        }
        (tableElem \ "constraints") foreach { constraintsElem =>
          (constraintsElem \ "primary-key") foreach { pkElem =>
            table.createPrimaryKey(pkElem("name"), split(pkElem("columns")).toSeq: _*)
          }
          (constraintsElem \ "foreign-key") foreach { fkElem =>
            val name = fkElem("name")
            val column = fkElem("column")
            val referTable = fkElem("referenced-table")
            val referColumn = fkElem("referenced-column")
            val fk = table.createForeignKey(name, column, database.refTable(referTable), referColumn)
            fkElem.get("enabled").foreach(n => fk.enabled = n.toBoolean)
            fkElem.get("cascadeDelete").foreach(n => fk.cascadeDelete = n.toBoolean)
          }
          (constraintsElem \ "unique-key") foreach { ukElem =>
            val uk = table.createUniqueKey(ukElem("name"), split(ukElem("columns")).toSeq: _*)
            ukElem.get("enabled").foreach(n => uk.enabled = n.toBoolean)
          }
        }
        (tableElem \ "indexes" \ "index") foreach { idxElem =>
          var unique = false
          idxElem.get("unique").foreach(n => unique = n.toBoolean)
          table.createIndex(idxElem("name"), unique, split(idxElem("columns")).toSeq: _*)
        }
        table.convertIndexToUniqueKeys()
      }

      (schemaElem \ "views" \ "view") foreach { viewElem =>
        val view = schema.createView(viewElem("name"))
        viewElem.get("comment").foreach(n => view.updateCommentAndModule(n))
        (viewElem \ "columns" \ "column") foreach { colElem =>
          val col = view.createColumn(colElem("name"), colElem("type"))
          colElem.get("nullable").foreach(n => col.nullable = n.toBoolean)
          colElem.get("unique").foreach(n => col.unique = n.toBoolean)
          colElem.get("check").foreach(n => col.check = Some(n))
          colElem.get("comment").foreach(n => col.comment = Some(n))
          colElem.get("defaultValue").foreach(n => col.defaultValue = Some(n))
        }
        (viewElem \ "definition") foreach { dfnElem =>
          if Strings.isNotBlank(dfnElem.text) then view.definition = Some(dfnElem.text.trim)
        }
      }
    }
    database
  }

  def toXml(database: Database): String = {
    new Exporter(database).toXml
  }

  private class Exporter(db: Database) {
    def toXml: String = {
      val dbNode = Element("db", ("engine", db.engine.name), ("version", db.version))
      if (db.schemas.nonEmpty) {
        val schemasNode = dbNode.append("schemas")
        val schemaNames = db.schemas.keys.toBuffer.sorted
        schemaNames foreach { schemaName =>
          val schema = db.schemas(schemaName)
          if (schema.tables.nonEmpty || schema.sequences.nonEmpty) {
            val schemaNode = schemasNode.append("schema", ("name", schemaName))
            if (schema.tables.nonEmpty) {
              val tablesNode = schemaNode.append("tables")
              schema.tables.values.toBuffer.sorted foreach { table =>
                appendXml(table, tablesNode)
              }
            }
            if (schema.views.nonEmpty) {
              val viewsNode = schemaNode.append("views")
              schema.views.values.toBuffer.sorted foreach { view =>
                appendXml(view, viewsNode)
              }
            }
          }
        }
      }
      dbNode.toXml
    }

    private def appendXml(table: Table, tablesNode: Element): Unit = {
      val tableNode = tablesNode.append("table", "name" -> table.name)
      tableNode.set("comment", table.commentAndModule)
      val columnsNode = tableNode.append("columns")
      val columns = table.columns.sortWith((c1, c2) => if (c1.name.value == "id") true else if (c2.name.value == "id") false else c1.name.value.compareTo(c2.name.value) < 0)
      columns foreach { col =>
        val colNode = columnsNode.append("column")
        colNode.set("name", col.name)
        colNode.set("type", col.sqlType.name)
        if (!col.nullable) {
          colNode.set("nullable", col.nullable.toString)
        }
        if (col.unique) {
          colNode.set("unique", col.unique.toString)
        }
        colNode.set("check", col.check)
        colNode.set("defaultValue", col.defaultValue)
        colNode.set("comment", col.comment)
      }
      if (table.primaryKey.isDefined || table.foreignKeys.nonEmpty || table.uniqueKeys.nonEmpty) {
        val constraintNode = tableNode.append("constraints")
        table.primaryKey foreach { pk =>
          val pkNode = constraintNode.append("primary-key")
          if null != pk.name && !pk.name.value.isBlank then pkNode.set("name", pk.name)
          pkNode.set("columns", collectNames(pk.columns))
        }
        table.foreignKeys foreach { fk =>
          val fkNode = constraintNode.append("foreign-key", "name" -> fk.name)
          fkNode.set("column", collectNames(fk.columns))
          fkNode.set("referenced-table", fk.referencedTable.qualifiedName)
          fkNode.set("referenced-column", collectNames(fk.referencedColumns))
          if (fk.cascadeDelete) {
            fkNode.set("cascade-delete", "true")
          }
          if (!fk.enabled) {
            fkNode.set("enabled", "false")
          }
        }
        table.uniqueKeys foreach { uk =>
          val ukNode = constraintNode.append("unique-key", "name" -> uk.name)
          ukNode.set("columns", collectNames(uk.columns))
          if !uk.enabled then ukNode.set("enabled", uk.enabled.toString)
        }
      }
      if (table.indexes.nonEmpty) {
        val idxNodes = tableNode.append("indexes")
        table.indexes foreach { idx =>
          val idxNode = idxNodes.append("index", "name" -> idx.name)
          idxNode.set("columns", collectNames(idx.columns))
          if (idx.unique) {
            idxNode.set("unique", idx.unique.toString)
          }
        }
      }
    }

    private def appendXml(view: View, viewsNode: Element): Unit = {
      val viewNode = viewsNode.append("view", "name" -> view.name)
      viewNode.set("comment", view.commentAndModule)
      val columnsNode = viewNode.append("columns")
      val columns = view.columns.sortWith((c1, c2) => if (c1.name.value == "id") true else if (c2.name.value == "id") false else c1.name.value.compareTo(c2.name.value) < 0)
      columns foreach { col =>
        val colNode = columnsNode.append("column")
        colNode.set("name", col.name)
        colNode.set("type", col.sqlType.name)
        if (!col.nullable) {
          colNode.set("nullable", col.nullable.toString)
        }
        if (col.unique) {
          colNode.set("unique", col.unique.toString)
        }
        colNode.set("check", col.check)
        colNode.set("defaultValue", col.defaultValue)
        colNode.set("comment", col.comment)
      }
      view.definition foreach { dfn =>
        viewNode.append("definition").inner(dfn)
      }
    }

    def collectNames(cols: Iterable[Identifier]): String = {
      cols.map(_.toLiteral(db.engine)).mkString(",")
    }

    implicit def identifier2String(i: Identifier): String = {
      i.toLiteral(db.engine)
    }
  }

}
