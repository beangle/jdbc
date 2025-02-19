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

package org.beangle.jdbc.query

import scala.reflect.ClassTag

case class ParamValue(value: Any, sqlType: Int)

object ParamValue {
  def of[T: ClassTag](o: Option[T])(implicit t: ClassTag[T]): Any = {
    o match
      case Some(t) => t
      case None => TypedNull(t.runtimeClass)
  }

  def nullOf(sqlType: Int): ParamValue = ParamValue(null, sqlType)

  def nullInt: ParamValue = ParamValue(null, java.sql.Types.INTEGER)

  def nullLong: ParamValue = ParamValue(null, java.sql.Types.BIGINT)

  def nullDouble: ParamValue = ParamValue(null, java.sql.Types.DOUBLE)

  def nullString: ParamValue = ParamValue(null, java.sql.Types.VARCHAR)

  def nullDate: ParamValue = ParamValue(null, java.sql.Types.DATE)

  def nullDateTime: ParamValue = ParamValue(null, java.sql.Types.TIMESTAMP)

  def nullInstant: ParamValue = ParamValue(null, java.sql.Types.TIMESTAMP_WITH_TIMEZONE)
}

case class TypedNull(clazz: Class[_])
