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

package org.beangle.jdbc.ds

import org.beangle.jdbc.meta.Identifier

/**
 * using serverName/database or url alternative
 */
class DatasourceConfig(val driver: String) {
  var name: String = _

  var user: String = _
  var password: String = _

  var props = new collection.mutable.HashMap[String, String]
  var schema: Option[Identifier] = None
  var catalog: Option[Identifier] = None

  def this(data: collection.Map[String, String]) = {
    this(data("driver"))
    data.foreach {
      case (k, v) =>
        k match {
          case "user" => this.user = v
          case "password" => this.password = v
          case "schema" => this.schema = Some(Identifier(v))
          case "catalog" => this.catalog = Some(Identifier(v))
          case "name" => this.name = v
          case "driver" =>
          case _ => props.put(k, v)
        }
    }
  }
}
