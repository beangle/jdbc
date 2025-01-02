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

package org.beangle.jdbc.query

import org.beangle.commons.conversion.string.BooleanConverter
import org.beangle.commons.lang.Strings

import java.io.Reader
import java.time.format.DateTimeFormatter
import java.time.{Instant, OffsetDateTime}

/** Convert data into postgresql style csv
 *
 * @param itor
 */
class PostgresCsvReader(itor: Iterator[Array[_]], types: collection.Seq[Int]) extends Reader {
  private val instantFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.Sx")

  private val zoneOffset = OffsetDateTime.now.getOffset
  private var buffer: Array[Char] = Array.ofDim[Char](16)
  private var bufLen: Int = 0
  private var index: Int = 0
  private val sqlTypes = types.toArray

  override def read(cbuf: Array[Char], off: Int, len: Int): Int = {
    if (itor.hasNext) {
      if (0 == bufLen) {
        val data = itor.next()
        if (null == data) {
          -1
        } else {
          makeString(data, sqlTypes)
          index = 0
          copy(cbuf, off, len)
        }
      } else {
        copy(cbuf, off, len)
      }
    } else {
      -1
    }
  }

  private def copy(cbuf: Array[Char], off: Int, len: Int): Int = {
    val readLen = Math.min(len, bufLen - index)
    System.arraycopy(buffer, index, cbuf, off, readLen)
    index += readLen
    if (index == bufLen) {
      bufLen = 0
      index = 0
    }
    readLen
  }

  private def makeString(data: Array[_], sqlTypes: Array[Int]): Unit = {
    val sb = new java.lang.StringBuilder()
    import java.sql.Types.*
    data.indices foreach { i =>
      var d: Any = data(i)
      val sqltype = sqlTypes(i)
      if (null == d) {
        sb.append(",")
      } else {
        if (sqltype == BOOLEAN) {
          d = d match
            case bln: Boolean => bln
            case n: Number => n.intValue() > 0
            case s: String => BooleanConverter.apply(s)
        }
        d.match {
          case b: Boolean => sb.append(if b then "t," else "f,")
          case i: Instant => sb.append(instantFormatter.format(i.atOffset(zoneOffset))).append(',')
          case s: String =>
            val str = Strings.replace(s, "\"", "'\"")
            val res = if (str.length > s.length) {
              s""""${str}""""
            } else if (str.contains(",")) {
              s""""${str}""""
            } else if (s.contains("\n")) {
              s""""${str}""""
            } else {
              str
            }
            sb.append(res).append(',')

          case a: Any => sb.append(a.toString).append(',')
        }
      }
    }

    sb.setCharAt(sb.length - 1, '\n') //remove last comma
    val len = sb.length
    if buffer.length < len then buffer = new Array[Char](len)
    sb.getChars(0, len, buffer, 0)
    bufLen = len
  }

  override def close(): Unit = {}
}
