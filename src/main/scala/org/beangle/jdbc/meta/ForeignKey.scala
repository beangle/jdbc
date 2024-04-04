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

import org.beangle.jdbc.engine.Engine

import scala.collection.mutable.ListBuffer

/**
  * JDBC foreign key metadata
  *
  * @author chaostone
  */
class ForeignKey(t: Table, n: Identifier, column: Identifier = null) extends Constraint(t, n) {

  var cascadeDelete: Boolean = false
  var referencedColumns = new ListBuffer[Identifier]
  var referencedTable: TableRef = _

  addColumn(column)

  override def toCase(lower: Boolean): Unit = {
    super.toCase(lower)
    val lowers = referencedColumns.map { col => col.toCase(lower) }
    referencedColumns.clear()
    referencedColumns ++= lowers

    if (this.table.schema == referencedTable.schema) referencedTable.toCase(lower)
  }

  override def attach(engine: Engine): Unit = {
    super.attach(engine)
    val changed = referencedColumns.map { col => col.attach(engine) }
    referencedColumns.clear()
    referencedColumns ++= changed

    if (this.table.schema == referencedTable.schema) referencedTable.name = referencedTable.name.attach(engine)

  }

  override def clone(): ForeignKey = {
    val cloned = super.clone().asInstanceOf[ForeignKey]
    cloned.cascadeDelete = this.cascadeDelete
    cloned.referencedTable = this.referencedTable
    var newColumns = new ListBuffer[Identifier]
    newColumns ++= referencedColumns
    cloned.referencedColumns = newColumns
    cloned
  }

  def refer(table: Table, cols: Identifier*): Unit = {
    this.referencedTable = TableRef(table.schema, table.name)
    if (cols.nonEmpty) referencedColumns ++= cols
  }

  def refer(table: TableRef, cols: Identifier*): Unit = {
    this.referencedTable = table
    if (cols.nonEmpty) referencedColumns ++= cols
  }

  override def toString: String = "Foreign key(" + name + ')'

  override def equals(other: Any): Boolean = {
    other match {
      case c: ForeignKey =>
        this.name == c.name && this.enabled == c.enabled && this.columns == c.columns &&
          this.cascadeDelete == c.cascadeDelete && this.referencedColumns == c.referencedColumns &&
          this.referencedTable.qualifiedName == c.referencedTable.qualifiedName
      case _ => false
    }
  }
}
