# Beangle Jdbc

The Beangle Jdbc Library is a lightweight JDBC toolkit for Scala 3, providing database engine abstraction, connection management, type mapping, query execution, metadata loading and SQL script execution.

## Features

- **Engine & dialect abstraction** for PostgreSQL, MySQL, MariaDB, Oracle, SQL Server, H2, DB2, Derby and HSQLDB, with automatic detection from `DataSource` metadata
- **Datasource management** based on HikariCP, configured from `datasources.xml` or a property map
- **JDBC type mapping** across engines (`SqlTypes`, `SqlTypeMapping`, `resolveCode`)
- **Query execution** via `JdbcExecutor`: query, paging fetch, update, batch insert (multi-value insert and PostgreSQL COPY)
- **Metadata loading** (`MetadataLoader`, `Database`, `DBScripts`, `Diff`) for introspection and schema diff
- **SQL script support**: parse scripts into structured `Statement` objects with comments and directives, execute via `Runner`
- **One-shot CLI** `org.beangle.jdbc.script.Main` with `sql.sh` / `sql.ps1` launchers

## Dependency

```scala
libraryDependencies += "org.beangle.jdbc" % "beangle-jdbc" % "1.1.12"
```

Requires Scala 3 and JDK 8+.

## Quick Start

### Build a datasource

```scala
import org.beangle.jdbc.ds.*

val conf = new DatasourceConfig("h2")
conf.props.put("url", "jdbc:h2:mem:test")
conf.props.put("user", "sa")
conf.props.put("password", "")

val ds = DataSourceUtils.build(conf)
```

Or parse from `datasources.xml`:

```xml
<datasources>
  <datasource name="default">
    <driver>h2</driver>
    <url>jdbc:h2:mem:test</url>
    <user>sa</user>
    <password></password>
  </datasource>
</datasources>
```

```scala
val conf = DataSourceUtils.parseXml(new FileInputStream("datasources.xml"))
val ds = DataSourceUtils.build(conf)
```

### Execute queries

```scala
import org.beangle.jdbc.query.JdbcExecutor
import org.beangle.commons.collection.page.PageLimit

val exec = new JdbcExecutor(ds)
exec.update("insert into users(name) values (?)", "tom")
val users: Seq[Array[Any]] = exec.query("select * from users")
val page: Seq[Array[Any]] = exec.fetch("select * from users", new PageLimit(1, 20))
```

### Run SQL scripts

```scala
import org.beangle.jdbc.engine.Engines
import org.beangle.jdbc.script.*

val source = org.beangle.jdbc.ds.Source(ds)
val parser = Parser.forEngine(source.engine)
val statements = Parser.readStatements(parser, new File("init.sql").toURI)
Runner.execute(ds, statements, ignoreError = true)
```

Parsed statements are structured objects carrying the SQL text, leading comments and directives:

```scala
class Statement(val sql: String, val comments: Seq[String] = Seq.empty, val directives: Seq[Directive] = Seq.empty)
```

### `@loop` directive

For `INSERT ... SELECT` over large tables, add a directive comment to run the statement in committed batches with an auto-appended `LIMIT`:

```sql
-- @loop batch-size=100000 max-batches=50 copy users
insert into users_archive select * from users
```

Supported parameters:

| Parameter     | Default | Description                         |
|---------------|---------|-------------------------------------|
| `batch-size`  | 100000  | Rows affected per batch             |
| `max-batches` | 50      | Maximum batches before aborting     |
| label         | -       | Optional label used in progress log |

The statement must be a plain `INSERT ... SELECT` without `LIMIT`, `ON CONFLICT` or `RETURNING`.

### Command-line execution

Provide a work directory containing `datasources.xml` and an `sql/` folder with `.sql` files:

```bash
sql.sh /path/to/workdir
```

```powershell
sql.ps1 C:\path\to\workdir
```

The launchers resolve dependencies from the local Maven repository (or an Aliyun mirror) and run every `.sql` file against every configured datasource.

## Modules

- `org.beangle.jdbc.engine` — engine/dialect abstraction, type mapping, reserved keywords
- `org.beangle.jdbc.ds` — datasource config, HikariCP integration
- `org.beangle.jdbc.query` — `JdbcExecutor`, batch insert, result-set iteration
- `org.beangle.jdbc.meta` — metadata loading, schema model, diff and DDL scripts
- `org.beangle.jdbc.script` — SQL script parsing and execution

## License

Beangle Jdbc is released under the [GNU Lesser General Public License v3](LICENSE).
