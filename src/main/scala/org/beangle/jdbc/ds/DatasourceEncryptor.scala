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

package org.beangle.jdbc.ds

object DatasourceEncryptor {
  def main(args: Array[String]): Unit = {
    if (args.length == 3) {
      val user = args(0)
      if (args(2).length == 32) {
        println(decrypt(user, args(1), args(2)))
      } else {
        println(new AesEncryptor(buildKey(user, args(1))).encrypt(args(2)))
      }
    } else {
      println("USAGE: java DatasourceEncryptor user url|servername plain|encoded")
    }
  }

  def decrypt(user: String, url: String, password: String): String = {
    new AesEncryptor(buildKey(user, url)).decrypt(password)
  }

  private def buildKey(user: String, url: String): String = {
    s"${user}${url} is the secret"
  }
}
