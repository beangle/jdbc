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
import org.beangle.commons.lang.Consoles.{prompt, readPassword, shell}
import org.beangle.commons.lang.Strings.*
import org.beangle.commons.lang.{Numbers, SystemInfo}
import org.beangle.commons.xml.Document
import org.beangle.jdbc.JdbcLogger
import org.beangle.jdbc.ds.{DataSourceUtils, DatasourceConfig}
import org.beangle.jdbc.engine.UrlFormat

import java.io.{File, FileInputStream}
import java.net.URL

object Sql {

  var datasource: DatasourceConfig = _

  var datasources = new collection.mutable.ListBuffer[DatasourceConfig]

  var workdir: String = _
  var sqlDir: String = _

  def main(args: Array[String]): Unit = {
    workdir = if (args.length == 0) SystemInfo.user.dir else args(0)
    read()
    sqlDir = workdir + / + "sql" + /
    if (sqlFiles.isEmpty) {
      println("Cannot find sql files in " + sqlDir)
      return
    }
    if (datasources.isEmpty) {
      JdbcLogger.info("Cannot find datasource")
      return
    }

    if (datasources.size == 1) datasource = datasources.head
    println("Sql executor:help ls exec exit(quit/q)")
    shell({
      val prefix =
        if (null != datasource) datasource.name
        else "sql"
      prefix + " >"
    }, Set("exit", "quit", "q"), {
      case "ls" => info()
      case "help" => printHelp()
      case t =>
        if (t.startsWith("use")) use(Numbers.toInt(trim(substringAfter(t, "use")), 0))
        else if (t.startsWith("exec")) exec(trim(substringAfter(t, "exec")))
        else if (isNotEmpty(t)) println(t + ": command not found...")
    })
  }

  def sqlFiles: Array[String] = {
    val file = new File(sqlDir)
    if (file.exists) {
      val files = for (f <- file.list() if f.endsWith(".sql")) yield f
      files.sorted
    } else Array.empty
  }

  def exec(file: String = null): Unit = {
    if (isBlank(file)) {
      println("Usage exec all or exec file1 file2")
    } else {
      var fileName = file
      if (file == "all") fileName = sqlFiles.mkString(" ")
      val urls = new collection.mutable.ListBuffer[URL]
      for (name <- split(fileName, " ")) {
        val f = new File(sqlDir + (if (name.endsWith(".sql")) name else name + ".sql"))
        if (f.exists) {
          urls += f.toURI.toURL
        } else {
          println("file " + f.getAbsolutePath + " doesn't exists")
        }
      }

      val runner = new Runner(OracleParser, urls.toSeq: _*)
      if (null == datasource) use(-1)
      if (null == datasource || urls.isEmpty) {
        println("Execute sql aborted.")
      } else {
        config(datasource)
        if (null == datasource.password)
          datasource.password = readPassword("enter datasource [%1$s] %2$s password:", datasource.name, datasource.user)

        val ds = DataSourceUtils.build(datasource.driver, datasource.user, datasource.password, datasource.props)
        runner.execute(ds, ignoreError = true)
      }
    }
  }

  def printHelp(): Unit = {
    println(
      """Avaliable command:
  ls                        print datasource and sql file
  exec [sqlfile1,sqlfile2]  execute simple file
  exec all                  execute all sql file
  help              print this help conent""")
  }

  def use(index: Int = 0): Unit = {
    if (datasources.isEmpty) {
      println("datasource is empty")
    } else {
      if (index > -1) {
        datasource = datasources(index)
      } else {
        val selectName = prompt("choose datasource index?", null, name => {
          val result = Numbers.convert2Int(name, null)
          result != null && result > -1 && result < datasources.size
        })
        datasource = datasources(Numbers.toInt(selectName))
      }
    }
  }

  def info(): Unit = {
    val infos = new collection.mutable.ListBuffer[String]
    var index = 0
    for (ds <- datasources) {
      val prefix = if (ds == datasource) "[*] " else "[" + index + "] "
      infos += prefix + ds.name
      index += 1
    }
    println("Data sources:\n" + ("-" * 50))
    println(infos.mkString("\n"))
    println()
    println("Sql files:\n" + ("-" * 50))
    println(sqlFiles.mkString(" "))
  }

  private def config(resource: DatasourceConfig): Unit = {
    var url = resource.props.get("url").orNull
    if (null == url) {
      val format = new UrlFormat(url)
      if (format.params.nonEmpty) {
        val params = format.params
        val values = new collection.mutable.HashMap[String, String]
        params.foreach { param => values.put(param, prompt("enter " + param + ":")) }
        url = format.fill(values.toMap)
        resource.props.put("url", url)
      }
      if (null == resource.user || resource.user == "<username>") {
        resource.user = prompt("enter datasource " + url + " username:")
      }
    }
  }

  private def read(): Unit = {
    assert(null != workdir)
    val target = new File(workdir + / + "datasources.xml")
    if (target.exists) {
      JdbcLogger.info(s"Read config file ${target.getName}")
      (Document.parse(target) \\ "datasource") foreach { elem =>
        datasources += DataSourceUtils.parseXml(elem)
      }
    }
  }
}
