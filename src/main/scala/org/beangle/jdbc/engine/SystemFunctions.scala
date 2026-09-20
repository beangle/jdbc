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

import org.beangle.jdbc.meta.SqlType
import scala.compiletime.uninitialized

class SystemFunctions {

  var currentDate: String = uninitialized

  var localTime: String = uninitialized
  var currentTime: String = uninitialized

  var localTimestamp: String = uninitialized
  var currentTimestamp: String = uninitialized

  def current(sqlType: SqlType): String = {
    sqlType.code match
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE => currentTimestamp
      case java.sql.Types.TIMESTAMP => localTimestamp
      case java.sql.Types.DATE => currentDate
      case java.sql.Types.TIME_WITH_TIMEZONE => currentTime
      case java.sql.Types.TIME => localTime
      case _ => null
  }
}
