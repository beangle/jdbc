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

/**
 * JDBC column metadata
 *
 * @author chaostone
 */
class Column(var name: Identifier, var sqlType: SqlType, var nullable: Boolean = true) extends Cloneable with Comment {

  var unique: Boolean = false
  var defaultValue: Option[String] = None
  var check: Option[String] = None

  def this(name: String, sqlType: SqlType) = {
    this(Identifier(name), sqlType)
  }

  override def clone(): this.type = {
    super.clone().asInstanceOf[this.type]
  }

  def toCase(lower: Boolean): Unit = {
    this.name = name.toCase(lower)
  }

  def hasCheck: Boolean = {
    check != null && check.isDefined
  }

  override def toString: String = {
    s"$name $sqlType"
  }

  def isSame(o: Column): Boolean = {
    this.name == o.name && this.sqlType.name == o.sqlType.name && this.nullable == o.nullable &&
      this.unique == o.unique && this.defaultValue == o.defaultValue && this.check == o.check
  }

  override def equals(other: Any): Boolean = {
    other match {
      case c: Column => isSame(c)
      case _ => false
    }
  }
}
