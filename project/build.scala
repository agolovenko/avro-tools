import sbt._

object Dependencies {
  private def apacheAvro       = "org.apache.avro"        % "avro"                     % "1.11.0"
  private def collectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1"
  private def playJson         = "com.typesafe.play"      %% "play-json"               % "2.9.3"
  private def scalaXml         = "org.scala-lang.modules" %% "scala-xml"               % "2.1.0"
  private def univocityParser  = "com.univocity"          % "univocity-parsers"        % "2.9.1"
  private def scalaTest        = "org.scalatest"          %% "scalatest"               % "3.2.14"

  def core: Seq[ModuleID] = Seq(
    collectionCompat % Compile,
    apacheAvro       % Compile,
    scalaTest        % Test
  )

  def json: Seq[ModuleID] = Seq(
    playJson  % Compile,
    scalaTest % Test
  )

  def xml: Seq[ModuleID] = Seq(
    scalaXml  % Compile,
    scalaTest % Test
  )

  def csv: Seq[ModuleID] = Seq(
    univocityParser % Compile,
    scalaTest       % Test
  )
}
