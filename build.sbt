import com.jsuereth.sbtpgp.PgpKeys.publishSigned
import sbt.Keys.scalaVersion
import sbt.Opts.resolver.{sonatypeSnapshots, sonatypeStaging}

lazy val scala213               = "2.13.10"
lazy val scala212               = "2.12.17"
lazy val supportedScalaVersions = Seq(scala213, scala212)

lazy val baseName = "avro-tools"

ThisBuild / organization := "io.github.agolovenko"
ThisBuild / homepage := Some(url("https://github.com/agolovenko/avro-tools"))
ThisBuild / scmInfo := Some(ScmInfo(url("https://github.com/agolovenko/avro-tools"), "git@github.com:agolovenko/avro-tools.git"))
ThisBuild / developers := List(Developer("agolovenko", "agolovenko", "ashotik@gmail.com", url("https://github.com/agolovenko")))
ThisBuild / licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / publishMavenStyle := true
ThisBuild / publishConfiguration := publishConfiguration.value.withOverwrite(true)
ThisBuild / publishTo := Some(if (isSnapshot.value) sonatypeSnapshots else sonatypeStaging)
ThisBuild / scalaVersion := scala213
ThisBuild / crossScalaVersions := supportedScalaVersions
ThisBuild / versionScheme := Some("early-semver")

ThisBuild / scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-encoding",
  "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused"
)

lazy val core = project
  .in(file("core"))
  .enablePlugins(GitVersioning)
  .settings(
    name := s"$baseName-core",
    libraryDependencies ++= Dependencies.core
  )

lazy val json = project
  .in(file("json"))
  .enablePlugins(GitVersioning)
  .settings(
    name := s"$baseName-json",
    libraryDependencies ++= Dependencies.json
  )
  .dependsOn(core)

lazy val xml = project
  .in(file("xml"))
  .enablePlugins(GitVersioning)
  .settings(
    name := s"$baseName-xml",
    libraryDependencies ++= Dependencies.xml
  )
  .dependsOn(core)

lazy val csv = project
  .in(file("csv"))
  .enablePlugins(GitVersioning)
  .settings(
    name := s"$baseName-csv",
    libraryDependencies ++= Dependencies.csv
  )
  .dependsOn(core)

lazy val root = project
  .in(file("."))
  .enablePlugins(GitVersioning)
  .settings(
    name := baseName,
    publish := {},
    publishLocal := {},
    publishSigned := {}
  )
  .aggregate(core, json, xml, csv)
