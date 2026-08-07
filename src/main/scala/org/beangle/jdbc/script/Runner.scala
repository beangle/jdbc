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

import org.beangle.commons.io.IOs
import org.beangle.commons.lang.Strings.{isBlank, lowerCase, substringAfter, substringBefore, trim}
import org.beangle.commons.lang.time.Stopwatch
import org.beangle.jdbc.JdbcLogger

import javax.sql.DataSource

object Runner {

  /** Execute parsed statements against a datasource.
   *  Returns true if every statement succeeded; with ignoreError=false failures are rethrown. */
  def execute(dataSource: DataSource, statements: Seq[Statement], ignoreError: Boolean): Boolean = {
    val watch = new Stopwatch(true)
    val conn = dataSource.getConnection()
    conn.setAutoCommit(true)
    val stm = conn.createStatement()
    var success = true
    try {
      val iter = statements.iterator
      while (iter.hasNext) {
        val statement = iter.next()
        val cmd = lowerCase(substringBefore(statement.sql, " "))
        if (Parser.commands.contains(cmd)) {
          if (cmd == "prompt") JdbcLogger.info(trim(substringAfter(statement.sql, cmd)))
          else JdbcLogger.info(statement.sql)
        } else {
          try {
            statement.directive(Directive.Loop) match {
              case Some(d) => executeLoop(stm, statement.sql, d)
              case None => stm.execute(statement.sql)
            }
          } catch {
            case e: Exception =>
              success = false
              JdbcLogger.error(s"Failure when exceute sql $statement.sql", e)
              if (!ignoreError) throw e
          }
        }
      }
    } finally {
      IOs.close(stm, conn)
    }
    JdbcLogger.info(s"exec sql using $watch")
    success
  }

  /** Execute an `@loop` INSERT...SELECT in committed batches with an auto appended LIMIT. */
  private def executeLoop(stm: java.sql.Statement, sql: String, directive: Directive): Unit = {
    val lowerSql = sql.toLowerCase.trim
    require(lowerSql.startsWith("insert"), "@loop only supports INSERT statements")
    require(raw"(?s).*\bselect\b.*".r.matches(lowerSql), "@loop only supports INSERT ... SELECT statements")
    require(!raw"(?s).*\blimit\b.*".r.matches(lowerSql), "@loop adds LIMIT automatically; remove LIMIT from the SQL")
    require(!raw"(?s).*\bon\s+conflict\b.*".r.matches(lowerSql), "@loop does not support ON CONFLICT")
    require(!raw"(?s).*\breturning\b.*".r.matches(lowerSql), "@loop does not support RETURNING")

    val batchSize = directive.param("batch-size").map(_.toInt).getOrElse(100000)
    val maxBatches = directive.param("max-batches").map(_.toInt).getOrElse(50)
    require(batchSize > 0, "batch-size must be greater than zero")
    require(maxBatches > 0, "max-batches must be greater than zero")

    val batchSql = s"$sql limit $batchSize"
    val label = if (isBlank(directive.label)) "loop insert" else directive.label
    val totalWatch = new Stopwatch(true)
    var batch = 0
    var total = 0L
    var affected = batchSize
    while (affected >= batchSize && batch < maxBatches) {
      batch += 1
      val batchWatch = new Stopwatch(true)
      affected = stm.executeUpdate(batchSql)
      total += affected
      JdbcLogger.info(s"$label batch $batch: $affected, total $total, using $batchWatch")
    }
    if (affected >= batchSize) {
      throw new IllegalStateException(s"$label reached max-batches $maxBatches after affecting $total rows")
    }
    JdbcLogger.info(s"$label completed: $total, using $totalWatch")
  }
}
