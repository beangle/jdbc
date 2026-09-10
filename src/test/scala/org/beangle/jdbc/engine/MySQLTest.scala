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

import org.beangle.jdbc.meta.{Column, Database, Identifier, Table}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Types

class MySQLTest extends AnyFlatSpec, Matchers {

  val mysql = new MySQL5

  "varchar mapping" should "keep short strings and promote long ones" in {
    mysql.toType(Types.VARCHAR, 500).name should be("varchar(500)")
    mysql.toType(Types.VARCHAR, 501).name should be("text")
  }

  "attach" should "demote varchars when the MySQL row would exceed 65535 bytes" in {
    val database = new Database(mysql)
    database.encoding = "utf8mb4"
    val schema = database.getOrCreateSchema("zsdx")
    val table = new Table(schema, Identifier("ht_ws_cgjbxx"))
    (1 to 40).foreach { i =>
      table.add(new Column(s"c$i", mysql.toType(Types.VARCHAR, 500)))
    }
    table.attach(mysql)
    table.columns.count(_.sqlType.name == "varchar(500)") should be < 40
    table.columns.exists(_.sqlType.name == "text") should be(true)
    mysql.createTable(table) should include("text")
  }
}
