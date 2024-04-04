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

import scala.collection.mutable

object Version {
  def apply(version: String): Version = {
    val containStart = !version.startsWith("(")
    val containEnd = !version.endsWith(")")
    var start: String = null
    var end: String = null

    val commaIndex: Int = version.indexOf(',')

    if (-1 == commaIndex) {
      start = version
      end = version
    } else {
      if ('[' == version.charAt(0) || '(' == version.charAt(0)) {
        start = version.substring(1, commaIndex)
      } else {
        start = version.substring(0, commaIndex)
      }
      if (']' == version.charAt(version.length() - 1) || ')' == version.charAt(version.length() - 1)) {
        end = version.substring(commaIndex + 1, version.length() - 1)
      } else {
        end = version.substring(commaIndex + 1)
      }

    }
    new Version(start, end, containStart, containEnd)
  }
}

/**
 * User [a,b] or (a,b) or a,b to discribe jdbc version range.
 * a,b should not all empty.
 *
 * @author chaostone
 */
class Version(start: String, end: String, containStart: Boolean, containEnd: Boolean) {

  def contains(v: String): Boolean = {
    if (Strings.isNotEmpty(start)) {
      val rs = start.compareTo(v)
      if ((!containStart && 0 == rs) || rs > 0) return false
    }
    if (Strings.isNotEmpty(end)) {
      val rs = end.compareTo(v)
      if ((!containEnd && 0 == rs) || rs < 0) return false
    }
    true
  }

  override def toString: String = {
    val sb = new mutable.StringBuilder()
    sb.append(if containStart then '[' else '(')
    sb.append(start)
    sb.append(",")
    sb.append(end)
    sb.append(if containEnd then ']' else ')')
    sb.mkString
  }
}
