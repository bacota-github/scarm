scalaVersion := "2.12.4"

scalacOptions ++= Seq("-Ypartial-unification", "-deprecation")

//val doobieVersion = "0.4.1"
val doobieVersion = "0.5.0-M9"

libraryDependencies ++= Seq(
    "org.tpolecat" %% "doobie-h2"        % doobieVersion, // H2 driver 1.4.196 + type mappings.
  "org.tpolecat" %% "doobie-core"      % doobieVersion,
  "org.tpolecat" %% "doobie-hikari"    % doobieVersion, // HikariCP transactor.
  "org.tpolecat" %% "doobie-postgres"  % doobieVersion, // Postgres driver 42.1.4 + type mappings.
//  "org.tpolecat" %% "doobie-specs2"    % doobieVersion, // Specs2 support for typechecking statements.
  "org.tpolecat" %% "doobie-scalatest" % doobieVersion  // ScalaTest support for typechecking statements.
)



