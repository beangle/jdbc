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

import scala.compiletime.uninitialized

class DBScripts {
  var schemas: List[String] = uninitialized
  var tables: List[String] = uninitialized
  var sequences: List[String] = uninitialized
  var keys:List[String]=uninitialized
  var constraints: List[String] = uninitialized
  var indices: List[String] = uninitialized
  var comments: List[String] = uninitialized
  var auxiliaries: List[String] = uninitialized
  var warnings:List[String]=uninitialized
}
