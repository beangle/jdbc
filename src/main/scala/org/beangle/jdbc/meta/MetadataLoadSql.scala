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

package org.beangle.jdbc.meta

import org.beangle.commons.lang.Strings

class MetadataLoadSql {

  var primaryKeySql: String = _

  var importedKeySql: String = _

  var indexInfoSql: String = _

  var sequenceSql: String = _

  var viewDefSql: String = _

  def supportsTableExtra: Boolean = {
    Strings.isNotBlank(primaryKeySql) && Strings.isNotBlank(importedKeySql) && Strings.isNotBlank(indexInfoSql)
  }

  def supportViewExtra: Boolean = {
    Strings.isNotBlank(viewDefSql)
  }

}
