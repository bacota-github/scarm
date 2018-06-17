scalaVersion := "2.12.6"
ensimeScalaVersion in ThisBuild := "2.12.6"

scalacOptions ++= Seq("-Ypartial-unification", "-deprecation", "-feature")// "-Xlog-implicits")

val doobieVersion = "0.5.2"

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.3",
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

//testOptions in Test += Tests.Argument("-oDF")
