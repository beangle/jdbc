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

package org.beangle.jdbc.aot

import com.zaxxer.hikari.HikariConfig
import org.beangle.commons.aot.{AotHintRegistrar, AotPolicy}
import org.beangle.commons.lang.ClassLoaders
import org.beangle.jdbc.query.JdbcExecutor

/** beangle-jdbc 自身的 GraalVM native-image 反射/资源提示。
 *
 * 构建期由 [[org.beangle.commons.aot.AotHintGenerator]] 扫描并生成
 * `META-INF/native-image` 配置，随 beangle-jdbc.jar 内嵌发布。
 * 涵盖：
 *  - [[JdbcExecutor]]：运行期按名/反射调用的查询执行器
 *  - HikariCP 连接池（`% optional` 依赖）：`PropertyElf` 经 `Class.getMethods` +
 *    setter 反射设置 `HikariConfig` 属性，连接池运行期还反射访问
 *    `PoolBase.catalog`/`PoolEntry.state` 等字段
 *  - PostgreSQL 驱动（`% optional` 依赖）：`HikariDataSource` 按
 *    `dataSourceClassName` 反射创建 `PGSimpleDataSource` 并经 `DriverManager`
 *    取连接，驱动内部还有 `Class.forName` 与字段反射
 *  - 资源：`org/beangle/jdbc/engine/keywords/.*`（各数据库保留字清单，
 *    [[org.beangle.jdbc.engine.Engine.loadKeywords]] 经 ClassLoader 加载），
 *    `org/postgresql/driverconfig.properties`（PG 驱动静态块读取）
 *
 * HikariCP/PostgreSQL 均为 optional 依赖，注册前先探测 classpath，避免
 * 未引入这些依赖的项目 AOT 生成失败。
 */
class JdbcAotHints extends AotHintRegistrar {

  /** 连接池/驱动类运行期反射面：构造器（Class.forName + newInstance）、
   *  public/declared 方法与字段（PropertyElf 的 getMethods/setter.invoke、
   *  直接字段 get/set）。 */
  private val fullPolicy = AotPolicy(Set(
    AotPolicy.Category.PublicMethods,
    AotPolicy.Category.DeclaredMethods,
    AotPolicy.Category.PublicConstructors,
    AotPolicy.Category.DeclaredConstructors,
    AotPolicy.Category.PublicFields,
    AotPolicy.Category.DeclaredFields))

  override def registering(): Unit = {
    hints.registerType(classOf[JdbcExecutor.type])
    registerPool()
    registerPostgresql()
    hints.registerPattern("org/beangle/jdbc/engine/keywords/.*")
  }

  /** HikariCP 连接池：HikariConfig 属性反射 + PoolBase/PoolEntry 字段反射。 */
  private def registerPool(): Unit = {
    val loader = getClass.getClassLoader
    ClassLoaders.get("com.zaxxer.hikari.HikariConfig", loader) foreach { _ =>
      hints.registerType(classOf[HikariConfig], fullPolicy)
      // PoolBase/PoolEntry 为 private[pool]，运行期仍会反射访问其字段，按名注册
      ClassLoaders.get("com.zaxxer.hikari.pool.PoolBase", loader) foreach (hints.registerType(_, fullPolicy))
      ClassLoaders.get("com.zaxxer.hikari.pool.PoolEntry", loader) foreach (hints.registerType(_, fullPolicy))
      // FastList 经 Array.newInstance 反射创建元素数组（PoolEntry 的
      // Statement[]、ConcurrentBag 的 IConcurrentBagEntry[]），数组类需
      // 注册并标记 unsafeAllocated
      hints.registerArrayOf("java.sql.Statement", loader)
      hints.registerArrayOf("com.zaxxer.hikari.util.ConcurrentBag$IConcurrentBagEntry", loader)
      // ProxyConnection.getProxy 对 java.sql.Connection 创建 JDK 动态代理
      // （关闭后调用抛 SQLException 的拦截器），需注册该代理接口
      hints.registerProxy(classOf[java.sql.Connection])
    }
  }

  /** PostgreSQL 驱动：dataSourceClassName 反射创建、DriverManager 驱动注册、
   *  Class.forName 加载连接工厂、以及驱动内部字段反射。 */
  private def registerPostgresql(): Unit = {
    val loader = getClass.getClassLoader
    if (ClassLoaders.get("org.postgresql.Driver", loader).nonEmpty) {
      hints.registerType(classOf[org.postgresql.Driver], fullPolicy)
      hints.registerType(classOf[org.postgresql.ds.PGSimpleDataSource], fullPolicy)
      hints.registerType(classOf[org.postgresql.ds.common.BaseDataSource], fullPolicy)
      hints.registerType(classOf[org.postgresql.core.v3.ConnectionFactoryImpl], fullPolicy)
      hints.registerType(classOf[org.postgresql.core.QueryExecutorCloseAction], fullPolicy)
      hints.registerType(classOf[org.postgresql.jdbc.PgStatement], fullPolicy)
      hints.registerType(classOf[org.postgresql.util.PGobject], fullPolicy)
      hints.registerPattern("org/postgresql/driverconfig\\.properties")
    }
  }
}
