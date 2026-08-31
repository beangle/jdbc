import org.beangle.parent.Dependencies.*
import org.beangle.parent.Settings.*
import sbt.Keys.libraryDependencies

organization := "org.beangle.jdbc"
version := "1.1.13"
scmInfo := Some(
  ScmInfo(
    uri("https://github.com/beangle/jdbc"),
    "scm:git@github.com:beangle/jdbc.git"
  )
)

developers := List(
  Developer(
    id = "chaostone",
    name = "Tihua Duan",
    email = "duantihua@gmail.com",
    url = uri("http://github.com/duantihua")
  )
)

description := "The Beangle Jdbc Library"
homepage := Some(uri("https://beangle.github.io/jdbc/index.html"))

val beangle_commons = "org.beangle.commons" % "beangle-commons" % "6.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "beangle-jdbc",
    common,
    Compile / mainClass := Some("org.beangle.jdbc.script.Main"),
    libraryDependencies ++= Seq(beangle_commons, slf4j, logback_classic % "test", scalatest),
    libraryDependencies ++= Seq(HikariCP % "optional", h2 % "test", postgresql % "optional",
      ojdbc11 % "optional", mysql_connector_java % "optional",
      "org.duckdb" % "duckdb_jdbc" % "1.5.5.1" % "test")
  )
