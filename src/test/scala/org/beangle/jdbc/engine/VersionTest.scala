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

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class VersionTest extends AnyFlatSpec with Matchers {

  "version [1.0,2.0] " should "contain both boundary" in {
    val version = Version("[1.0,2.0]");
    version.contains("1.0") should be(true)
    version.contains("1.0.1") should be(true)
    version.contains("1.5.2") should be(true)
    version.contains("2.0") should be(true)
    version.contains("0.9.beta") should be(false)
    version.contains("2.0.1") should be(false)
  }

  "single version" should "contain only one version" in {
    val version = Version("2.3.5");
    version.contains("2.3") should be(false)
    version.contains("2.4") should be(false)
    version.contains("2.3.5") should be(true)
  }

  "open range without end" should "contains version greate than start" in {
    val version = Version("(2.3.5,)")
    version.contains("2.3") should be(false)
    version.contains("2.4") should be(true)
    version.contains("2.5") should be(true)
  }

  "open range without start " should "contains version less than end" in {
    val version = Version("(,2.3.5]")
    version.contains("2.3.5") should be(true)
    version.contains("2.3") should be(true)
    version.contains("2.5") should be(false)
  }
}
