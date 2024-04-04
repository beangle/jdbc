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

import org.beangle.jdbc.engine.Options.TableOptions

object Options {

  class AlterTableOption {
    var changeType: String = _
    var setNotNull: String = _
    var dropNotNull: String = _
    var setDefault: String = _
    var dropDefault: String = _
    var addPrimaryKey: String = _
    var dropPrimaryKey: String = _
    var dropConstraint: String = _
    var addColumn: String = _
    var dropColumn: String = _
    var renameColumn: String = _
  }

  class CreateTableOption {
    var supportsColumnCheck = true
  }

  class CommentOption {
    var supportsCommentOn = true
  }

  class DropTableOption {
    var sql = "drop table {name}"
  }

  class TruncateTableOption {
    var sql = "truncate table {name}"
  }

  class ConstraintOption {
    var supportsCascadeDelete = true
  }

  class LimitOption {
    var pattern: String = _
    var offsetPattern: String = _
    var bindInReverseOrder: Boolean = _
  }

  class SequenceOption {
    var supports = true
    var createSql: String = "create sequence {name} start with {start} increment by {increment} {cycle}"
    var dropSql: String = "drop sequence {name}"
    var nextValSql: String = _
    var selectNextValSql: String = _
  }

  class TableOptions {
    val alter = new AlterTableOption
    val create = new CreateTableOption
    val drop = new DropTableOption
    val truncate = new Options.TruncateTableOption

    def create(f: Options.CreateTableOption => Unit): Unit = {
      f(this.create)
    }

    def alter(f: Options.AlterTableOption => Unit): Unit = {
      f(this.alter)
    }

    def validate(): Unit = {
      require(null != alter.changeType, "Alter column type sql is required")
      require(null != alter.setNotNull, "Alter column set not null sql is required")
      require(null != alter.dropNotNull, "Alter column drop not null sql is required")
      require(null != alter.setDefault, "Alter column set default sql is required")
      require(null != alter.dropDefault, "Alter column drop default sql is required")
      require(null != alter.addColumn, "Add column sql is required")
      require(null != alter.dropColumn, "Drop column sql is required")

      require(null != alter.addPrimaryKey, "Add primary key sql is required")
      require(null != alter.dropConstraint, "Drop constraint sql is required")
    }
  }
}

class Options {

  val table = new TableOptions()

  var comment = new Options.CommentOption

  var constraint = new Options.ConstraintOption

  var limit = new Options.LimitOption

  var sequence = new Options.SequenceOption

  def sequence(f: Options.SequenceOption => Unit): Unit = {
    f(this.sequence)
  }

  def limit(f: Options.LimitOption => Unit): Unit = {
    f(this.limit)
  }

  def validate(): Unit = {
    table.validate()
  }
}
