import org.beangle.parent.Dependencies.*
import org.beangle.parent.Settings.*
import sbt.Keys.libraryDependencies

ThisBuild / organization := "org.beangle.jdbc"
ThisBuild / version := "1.1.6-SNAPSHOT"
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/beangle/jdbc"),
    "scm:git@github.com:beangle/jdbc.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id = "chaostone",
    name = "Tihua Duan",
    email = "duantihua@gmail.com",
    url = url("http://github.com/duantihua")
  )
)

ThisBuild / description := "The Beangle Jdbc Library"
ThisBuild / homepage := Some(url("https://beangle.github.io/jdbc/index.html"))

val beangle_commons = "org.beangle.commons" % "beangle-commons" % "5.7.0"
val commonDeps = Seq(beangle_commons, slf4j, logback_classic % "test", scalatest)

lazy val root = (project in file("."))
  .settings(
    name := "beangle-jdbc",
    common,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= Seq(scalaxml, HikariCP % "optional", h2 % "test", postgresql % "optional")
  )
