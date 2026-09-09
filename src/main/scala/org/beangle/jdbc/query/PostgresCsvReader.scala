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
    // Drain leftover bytes first. The last row may still be in `buffer` after
    // the iterator is exhausted; CopyManager reads in chunks (typically 64KiB).
    if (0 == bufLen) {
      if (!itor.hasNext) return -1
      val data = itor.next()
      if (null == data) return -1
      makeString(data, sqlTypes)
      index = 0
    }
    copy(cbuf, off, len)
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
            appendCsvField(sb, s)
            sb.append(',')
          case a: Any =>
            appendCsvField(sb, a.toString)
            sb.append(',')
        }
      }
    }

    sb.setCharAt(sb.length - 1, '\n') //remove last comma
    val len = sb.length
    if buffer.length < len then buffer = new Array[Char](len)
    sb.getChars(0, len, buffer, 0)
    bufLen = len
  }

  /** Quote a field per PostgreSQL COPY CSV rules (QUOTE '"', ESCAPE '"').
   *
   * Quote empty values (to distinguish from NULL) and values containing the
   * delimiter, quote, CR, or LF. Double any quote characters inside the field.
   *
   * @see https://www.postgresql.org/docs/current/sql-copy.html
   */
  private def appendCsvField(sb: java.lang.StringBuilder, s: String): Unit = {
    var needsQuote = s.isEmpty
    if (!needsQuote) {
      var i = 0
      while (i < s.length && !needsQuote) {
        val c = s.charAt(i)
        needsQuote = c == ',' || c == '"' || c == '\n' || c == '\r'
        i += 1
      }
    }
    if (needsQuote) {
      sb.append('"')
      var i = 0
      while (i < s.length) {
        val c = s.charAt(i)
        if (c == '"') sb.append('"')
        sb.append(c)
        i += 1
      }
      sb.append('"')
    } else {
      sb.append(s)
    }
  }

  override def close(): Unit = {}
}
