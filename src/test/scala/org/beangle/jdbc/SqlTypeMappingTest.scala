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

package org.beangle.jdbc

import org.beangle.commons.lang.annotation.value
import org.beangle.commons.lang.math.{Decimal5, TinyDecimal5}
import org.beangle.jdbc.engine.Engines
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Types

class SqlTypeMappingTest extends AnyFunSpec, Matchers {
  describe("SqlTypeMapping") {
    it("test value type") {
      val mapping = new DefaultSqlTypeMapping(Engines.forName("h2"))
      assert(mapping.sqlCode(classOf[Terms]) == Types.SMALLINT)
      assert(mapping.sqlCode(Meta.A.getClass) == Types.INTEGER)
      assert(mapping.sqlCode(classOf[Array[Byte]]) == Types.VARBINARY)
    }

    it("maps Decimal5 and TinyDecimal5 to DECIMAL") {
      val mapping = new DefaultSqlTypeMapping(Engines.forName("h2"))
      mapping.sqlCode(classOf[Decimal5]) shouldBe Types.DECIMAL
      mapping.sqlCode(classOf[TinyDecimal5]) shouldBe Types.DECIMAL

      mapping.sqlCode(Decimal5.of("12.34567").getClass) shouldBe Types.DECIMAL
      mapping.sqlCode(TinyDecimal5.of("12.34567").getClass) shouldBe Types.DECIMAL

      mapping.sqlType(classOf[Decimal5]).code shouldBe Types.DECIMAL
      mapping.sqlType(classOf[TinyDecimal5]).code shouldBe Types.DECIMAL
    }
  }
}

@value
class Terms(value: Short)

enum Meta {
  case A, B, C
}
