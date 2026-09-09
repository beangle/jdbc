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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Types

class PostgresCsvReaderTest extends AnyFlatSpec, Matchers {

  private def readAll(reader: PostgresCsvReader, chunk: Int = 8): String = {
    val out = new java.lang.StringBuilder()
    val buffer = Array.ofDim[Char](chunk)
    var n = reader.read(buffer, 0, buffer.length)
    while (n != -1) {
      out.append(buffer, 0, n)
      n = reader.read(buffer, 0, buffer.length)
    }
    out.toString
  }

  "PostgresCsvReader" should "distinguish null from an empty string" in {
    val rows = Iterator(Array[Any](null, "", "text"))
    val reader = new PostgresCsvReader(rows, Seq.fill(3)(Types.VARCHAR))

    readAll(reader) shouldBe ",\"\",text\n"
  }

  it should "escape quoted strings without treating them as null" in {
    val rows = Iterator(Array[Any]("\"\"", "a,b", "a\nb"))
    val reader = new PostgresCsvReader(rows, Seq.fill(3)(Types.VARCHAR))

    // "" → """""" (quote the field and double each quote)
    readAll(reader) shouldBe "\"\"\"\"\"\",\"a,b\",\"a\nb\"\n"
  }

  it should "quote carriage returns and apostrophes so COPY does not split columns" in {
    val rows = Iterator(Array[Any]("a\rb", "foo,bar'", "ok"))
    val reader = new PostgresCsvReader(rows, Seq.fill(3)(Types.VARCHAR))

    readAll(reader) shouldBe "\"a\rb\",\"foo,bar'\",ok\n"
  }

  it should "drain leftover buffer after the last row" in {
    val long = "x" * 100
    val reader = new PostgresCsvReader(Iterator(Array[Any](long)), Seq(Types.VARCHAR))

    readAll(reader) shouldBe long + "\n"
  }
}
