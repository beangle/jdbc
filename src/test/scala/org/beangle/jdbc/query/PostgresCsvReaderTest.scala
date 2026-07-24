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

  "PostgresCsvReader" should "distinguish null from an empty string" in {
    val rows = Iterator(Array[Any](null, "", "text"))
    val reader = new PostgresCsvReader(rows, Seq.fill(3)(Types.VARCHAR))
    val buffer = Array.ofDim[Char](64)

    val length = reader.read(buffer, 0, buffer.length)

    new String(buffer, 0, length) shouldBe ",\"\",text\n"
  }

  it should "escape quoted strings without treating them as null" in {
    val rows = Iterator(Array[Any]("\"\"", "a,b", "a\nb"))
    val reader = new PostgresCsvReader(rows, Seq.fill(3)(Types.VARCHAR))
    val buffer = Array.ofDim[Char](64)

    val length = reader.read(buffer, 0, buffer.length)

    new String(buffer, 0, length) shouldBe "\"'\"'\"\",\"a,b\",\"a\nb\"\n"
  }
}
