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

import org.beangle.commons.io.Files./
import org.beangle.commons.lang.SystemInfo
import org.beangle.commons.xml.Document
import org.beangle.jdbc.JdbcLogger
import org.beangle.jdbc.ds.{DataSourceUtils, DatasourceConfig}
import org.beangle.jdbc.script.Runner

import java.io.File

/** One-shot SQL executor: parse datasources.xml, run the given sql files, then exit. */
object Main {

  def main(args: Array[String]): Unit = {
    val workdir = if (args.isEmpty) SystemInfo.user.dir else args(0)
    val sqlDir = workdir + / + "sql"

    val datasources = readDatasources(workdir)
    if (datasources.isEmpty) {
      JdbcLogger.error(s"Cannot find datasource in $workdir/datasources.xml")
      return
    }
    val files = sqlFiles(sqlDir)
    if (files.isEmpty) {
      JdbcLogger.error(s"Cannot find sql files in $sqlDir")
      return
    }

    var total = 0
    for (ds <- datasources) {
      val source = org.beangle.jdbc.ds.Source(ds)
      try {
        files.foreach { f =>
          val statements = Parser.readStatements(Parser.forEngine(source.engine), f.toURI.toURL)
          if (statements.nonEmpty) {
            JdbcLogger.info(s"executing ${statements.size} statements from $f")
            Runner.execute(source.dataSource, statements, ignoreError = true)
            total += statements.size
          }
        }
      } finally {
        DataSourceUtils.close(source.dataSource)
      }
    }
    JdbcLogger.info(s"executed $total statements")
  }

  private def sqlFiles(sqlDir: String): List[File] = {
    val dir = new File(sqlDir)
    if (!dir.exists() || !dir.isDirectory) List.empty
    else {
      val files = for (f <- dir.listFiles() if f.getName.endsWith(".sql")) yield f
      files.toList.sortBy(_.getName)
    }
  }

  private def readDatasources(workdir: String): List[DatasourceConfig] = {
    val target = new File(workdir + / + "datasources.xml")
    if (!target.exists()) List.empty
    else {
      JdbcLogger.info(s"Read config file ${target.getName}")
      (Document.parse(target) \\ "datasource").map { elem => DataSourceUtils.parseXml(elem) }.toList
    }
  }
}
