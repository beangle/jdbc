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

import org.beangle.commons.io.IOs
import org.beangle.commons.json.JsonObject
import org.beangle.commons.lang.ClassLoaders
import org.beangle.jdbc.JdbcLogger
import org.beangle.jdbc.ds.DataSourceUtils
import org.beangle.jdbc.meta.{Database, Identifier, MetadataLoader, Schema}
import org.beangle.jdbc.query.JdbcExecutor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class H2Test extends AnyFlatSpec, Matchers {
  protected var schema: Schema = _

  protected def listTableAndSequences = {
    val tables = schema.tables
    for (name <- tables.keySet) {
      JdbcLogger.info(s"table $name")
    }

    val seqs = schema.sequences
    for (obj <- seqs) {
      JdbcLogger.info(s"sequence $obj")
    }
  }

  val properties = ClassLoaders.getResource("db.properties") match {
    case Some(r) => IOs.readJavaProperties(r)
    case None => Map.empty[String, String]
  }

  "h2 " should "load tables and sequences" in {
    val ds = DataSourceUtils.build("h2", properties("h2.username"), properties("h2.password"), Map("url" -> properties("h2.url")))

    val executor = new JdbcExecutor(ds)
    executor.update("create table AA(id integer,name json)")
    executor.update("""insert into AA values(1,'{"id":3}' format json)""")
    executor.update("insert into AA values(2,?)", JsonObject("id" -> 4999999L))
    val rs = executor.query("select * from AA order by id")
    assert(rs.length == 2)
    assert(rs.last.last == """{"id":"4999999"}""")
    val meta = ds.getConnection().getMetaData
    val engine = Engines.forDataSource(ds)
    val database = new Database(engine)
    schema = database.getOrCreateSchema(Identifier("PUBLIC"))
    val loader = new MetadataLoader(meta, engine)
    loader.loadTables(schema, false)
    loader.loadSequences(schema)
    listTableAndSequences
  }
}
