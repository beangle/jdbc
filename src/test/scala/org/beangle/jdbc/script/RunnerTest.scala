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

package org.beangle.jdbc.script

import org.h2.jdbcx.JdbcDataSource
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class RunnerTest extends AnyFunSpec, Matchers {
  describe("Runner") {
    it("executes loop insert in committed batches") {
      val ds = new JdbcDataSource
      ds.setURL("jdbc:h2:mem:runner_loop;DB_CLOSE_DELAY=-1")
      val conn = ds.getConnection
      try {
        conn.createStatement().execute("create table source_data(id int primary key)")
        conn.createStatement().execute("create table target_data(id int primary key)")
        conn.createStatement().execute("insert into source_data select x from system_range(1, 7)")
      } finally {
        conn.close()
      }

      val tmp = File.createTempFile("loop", ".sql")
      tmp.deleteOnExit()
      java.nio.file.Files.writeString(tmp.toPath,
        """-- @loop batch-size=3 max-batches=10 import target_data
insert into target_data(id)
select s.id from source_data s
where not exists(select 1 from target_data t where t.id=s.id)
;""")

      val statements = Parser.read(OracleParser, tmp.toURI).flatMap(_.statements)
      Runner.execute(ds, statements, ignoreError = false)

      val verify = ds.getConnection
      try {
        val rs = verify.createStatement().executeQuery("select count(*) from target_data")
        rs.next() shouldBe true
        rs.getInt(1) shouldBe 7
      } finally {
        verify.close()
      }
    }

    it("executes DML statements with JDBC update counts") {
      val ds = new JdbcDataSource
      ds.setURL("jdbc:h2:mem:runner_dml;DB_CLOSE_DELAY=-1")
      val conn = ds.getConnection
      try conn.createStatement().execute("create table staffs(code varchar(20), department_id int)")
      finally conn.close()

      val sql =
        """insert into staffs values('298437', 1);
          |update staffs set department_id=9 where code in('298437');
          |delete from staffs where code in('missing');
          |""".stripMargin
      val counts = collection.mutable.ListBuffer.empty[Int]
      Runner.execute(ds, new Parser().parse(sql), ignoreError = false, onUpdate = { (_, n, _) =>
        counts += n
      }) shouldBe true
      counts.toList shouldBe List(1, 1, 0)

      val verify = ds.getConnection
      try {
        val rs = verify.createStatement().executeQuery("select department_id from staffs where code='298437'")
        rs.next() shouldBe true
        rs.getInt(1) shouldBe 9
      } finally {
        verify.close()
      }
    }
  }
}
