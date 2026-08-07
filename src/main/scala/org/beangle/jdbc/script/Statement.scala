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

package org.beangle.jdbc.script

object Directive {
  val Loop = "loop"
}

/** A directive extracted from a leading comment line, e.g. `-- @loop batch-size=100000 max-batches=50 label`. */
case class Directive(name: String, params: Map[String, String] = Map.empty, label: String = "") {
  def param(name: String): Option[String] = params.get(name)
}

/** A parsed SQL statement with its leading comments and directives. */
class Statement(val sql: String, val comments: Seq[String] = Seq.empty, val directives: Seq[Directive] = Seq.empty) {
  def directive(name: String): Option[Directive] = directives.find(_.name == name)
}

class Script(val source: Any, val statements: List[Statement]) {

}
