scalaVersion := "2.12.6"
ensimeScalaVersion in ThisBuild := "2.12.6"
version := "0.2.0"

scalacOptions ++= Seq("-Ypartial-unification", "-deprecation", "-feature")// "-Xlog-implicits")

val doobieVersion = "0.5.3"

libraryDependencies ++= Seq(
  "org.tpolecat" %% "doobie-h2"        % doobieVersion, // H2 driver 1.4.196 + type mappings.
  "org.tpolecat" %% "doobie-core"      % doobieVersion,
  "org.tpolecat" %% "doobie-hikari"    % doobieVersion, // HikariCP transactor.
//  "org.tpolecat" %% "doobie-specs2"    % doobieVersion, // Specs2 support for typechecking statements.
  "org.tpolecat" %% "doobie-scalatest" % doobieVersion,  // ScalaTest support for typechecking statements.

  //"org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "ai.x" %% "typeless" % "0.5.0",
  "org.hsqldb" % "hsqldb" % "2.4.0" % "test",
  "org.tpolecat" %% "doobie-postgres"  % doobieVersion % "test",
  "mysql" % "mysql-connector-java" % "5.1.46" % "test"
)

testOptions in Test += Tests.Argument("-oDF")

// POM settings for Sonatype
organization := "com.vivi"
homepage := Some(url("https://github.com/bacota-github/scarm"))
scmInfo := Some(ScmInfo(url("https://github.com/bacota-github/scarm"),
                            "git@github.com:bacota-github/scarm.git"))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
developers := List(
  Developer(
    id = "bacota-github",
    name = "Bruce A Cota",
    email = "sonatype@vivi.com",
    url = url("https://github.com/bacota-github/scarm")
  )
)

publishMavenStyle := true

pomIncludeRepository := { _ => false }

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

