import sbt._

object Dependencies {
  private def jacksonVersion = "2.13.4"

  private def jacksonCore           = "com.fasterxml.jackson.core"       % "jackson-core"            % jacksonVersion
  private def jacksonAnnotations    = "com.fasterxml.jackson.core"       % "jackson-annotations"     % jacksonVersion
  private def jacksonDatabind       = "com.fasterxml.jackson.core"       % "jackson-databind"        % jacksonVersion
  private def jacksonDatatypeJdk8   = "com.fasterxml.jackson.datatype"   % "jackson-datatype-jdk8"   % jacksonVersion
  private def jacksonDatatypeJsr310 = "com.fasterxml.jackson.datatype"   % "jackson-datatype-jsr310" % jacksonVersion

  private def apacheAvro       = "org.apache.avro"        % "avro"                     % "1.11.0"
  private def collectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1"
  private def playJson         = "com.typesafe.play"      %% "play-json"               % "2.9.3"
  private def scalaXml         = "org.scala-lang.modules" %% "scala-xml"               % "2.1.0"
  private def univocityParser  = "com.univocity"          % "univocity-parsers"        % "2.9.1"

  private def scalaTest = "org.scalatest" %% "scalatest" % "3.2.14"

  def core: Seq[ModuleID] = Seq(
    collectionCompat   % Compile,
    apacheAvro         % Compile,
    jacksonCore        % Compile,
    jacksonAnnotations % Compile,
    jacksonDatabind    % Compile,
    scalaTest          % Test
  )

  def json: Seq[ModuleID] = Seq(
    jacksonDatatypeJdk8   % Compile,
    jacksonDatatypeJsr310 % Compile,
    playJson              % Compile,
    scalaTest             % Test
  )

  def xml: Seq[ModuleID] = Seq(
    scalaXml             % Compile,
    scalaTest            % Test
  )

  def csv: Seq[ModuleID] = Seq(
    univocityParser % Compile,
    scalaTest       % Test
  )
}
