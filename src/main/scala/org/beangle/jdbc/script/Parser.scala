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

import org.beangle.commons.io.{IOs, StringBuilderWriter}
import org.beangle.commons.lang.Charsets
import org.beangle.commons.lang.Strings.*
import org.beangle.jdbc.engine.Engine

import java.io.{InputStream, InputStreamReader}
import java.net.URI

object Parser {
  /** Meta commands that are logged instead of being executed as SQL. */
  val commands: Set[String] = Set("set", "prompt", "exit")

  /** Read and parse script files into [[Script]]s. */
  def read(parser: Parser, uris: URI*): List[Script] = {
    val buf = new collection.mutable.ListBuffer[Script]
    for (uri <- uris) {
      var in: InputStream = null
      try {
        in = uri.toURL.openStream()
        val sw = new StringBuilderWriter(16)
        IOs.copy(new InputStreamReader(in, Charsets.UTF_8), sw)
        buf += new Script(uri, parser.parse(sw.toString))
      } finally {
        IOs.close(in)
      }
    }
    buf.toList
  }

  /** Read and parse script files into flattened statements. */
  def readStatements(parser: Parser, uris: URI*): List[Statement] = {
    read(parser, uris: _*).flatMap(_.statements)
  }

  def forEngine(engine: Engine): Parser = {
    if (engine.name.toLowerCase.contains("oracle")) {
      OracleParser
    } else {
      new Parser
    }
  }
}

class Parser {
  def parse(content: String): List[Statement] = {
    val lines = split(content, "\n")
    val buf = new collection.mutable.ListBuffer[Statement]
    val stateBuf = new collection.mutable.ListBuffer[String]
    val comments = new collection.mutable.ListBuffer[String]
    var tails: Seq[String] = List.empty
    for (l <- lines; line = trim(l); if isNotBlank(l)) {
      if (isComment(line)) {
        if (tails.isEmpty) comments += line
      } else {
        if (tails.isEmpty) tails = endOf(line)
        if (stateBuf.nonEmpty) stateBuf += "\n"
        stateBuf += line
        val iter = tails.iterator
        while (tails.nonEmpty && iter.hasNext) {
          val tail = iter.next()
          if (line.endsWith(tail)) {
            val sql = if (tail.nonEmpty) substringBeforeLast(stateBuf.mkString, tail) else stateBuf.mkString
            buf += new Statement(sql.trim, comments.toList, extractDirectives(comments.toList))
            stateBuf.clear()
            comments.clear()
            tails = List.empty
          }
        }
      }
    }
    buf.toList
  }

  def isComment(line: String): Boolean = line.startsWith("--")

  def endOf(line: String): Seq[String] = List(";")

  /** Extract directives from leading comments. */
  def extractDirectives(comments: Seq[String]): Seq[Directive] = comments.flatMap(parseDirective)

  /** Parse a directive comment like `-- @loop batch-size=100000 max-batches=50 label`. */
  def parseDirective(comment: String): Option[Directive] = {
    val content = comment.stripPrefix("--").trim
    if (!content.startsWith("@")) None
    else {
      val tokens = split(content, " ")
      val name = tokens.head.stripPrefix("@").toLowerCase
      if (tokens.length == 1) Some(Directive(name))
      else {
        val params = tokens.drop(1).takeWhile(_.contains("=")).map { token =>
          val idx = token.indexOf('=')
          token.substring(0, idx).trim.toLowerCase -> token.substring(idx + 1).trim
        }.toMap
        val label = tokens.drop(1).dropWhile(_.contains("=")).mkString(" ")
        Some(Directive(name, params, label))
      }
    }
  }
}

object OracleParser extends Parser {

  override def endOf(line: String): Seq[String] = {
    val lower = line.toLowerCase()
    val cmd = substringBefore(lower, " ")
    if (Parser.commands.contains(cmd)) List("", ";")
    else if (lower.matches("create(.*?) package (.*?)")) List("/")
    else if (lower.matches("create(.*?) type (.*?)")) List("/")
    else if (lower.matches("create(.*?) function (.*?)")) List("/")
    else if (lower.matches("create(.*?) procedure (.*?)")) List("/")
    else List(";")
  }
}
