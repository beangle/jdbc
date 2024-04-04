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

import org.beangle.jdbc.engine.{Engines, PostgreSQL10}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Types

class DiffTest extends AnyFunSpec with Matchers {
  val engine = new PostgreSQL10

  describe("Diff") {
    it("column diff") {
      val column1 = new Column("id", engine.toType(Types.VARCHAR, 30))
      val column2 = new Column("id", engine.toType(Types.VARCHAR, 31))
      column1 should not be column2
    }

    it("table diff") {
      val id1 = new Column("id", engine.toType(Types.BIGINT))
      val id2 = new Column("id", engine.toType(Types.INTEGER))
      val name = new Column("name", engine.toType(Types.VARCHAR, 200))
      val code1 = new Column("code", engine.toType(Types.VARCHAR, 20))
      val code2 = new Column("code", engine.toType(Types.VARCHAR, 20))
      code2.unique = true

      val database = new Database(engine)
      val schema = database.getOrCreateSchema("public")
      val table1 = new Table(schema, Identifier("users"))
      val table2 = new Table(schema, Identifier("users"))
      table1.add(id1, name, code1)
      table2.add(id2, name, code2)

      val rs = Diff.diff(table2, table1, engine)
      rs shouldBe defined
      rs foreach { tableDiff =>
        tableDiff.columns.updated should have size 2
        println(tableDiff.columns)
      }
    }
  }
}
