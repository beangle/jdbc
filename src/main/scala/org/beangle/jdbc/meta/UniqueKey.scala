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
  * Unique Key
  *
  * @author chaostone
  */
class UniqueKey(table: Table, n: Identifier) extends Constraint(table, n) {

  override def clone(): UniqueKey = {
    super.clone().asInstanceOf[UniqueKey]
  }

  override def equals(other: Any): Boolean = {
    other match {
      case c: UniqueKey =>
        this.name == c.name && this.enabled == c.enabled && this.columns == c.columns
      case _ => false
    }
  }
}
