import com.typesafe.tools.mima.core._

inThisBuild(
  List(
    homepage := Some(url("https://pityka.github.io/")),
    licenses := List(("MIT", url("https://opensource.org/licenses/MIT"))),
    developers := List(
      Developer(
        "pityka",
        "Istvan Bartha",
        "bartha.pityu@gmail.com",
        url("https://github.com/pityka/relational3")
      )
    )
  )
)

lazy val scalaVersion213 = "2.13.14"
lazy val scalaVersion3 = "3.5.1"
lazy val scalaVersionInBuild = scalaVersion3

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / versionPolicyIntention := Compatibility.None
ThisBuild / versionPolicyIgnoredInternalDependencyVersions := Some(
  raw"^\d+\.\d+\.\d+\+\d+".r
) // Support for versions generated by sbt-dynver

lazy val commonSettings = Seq(
  crossScalaVersions := Seq(scalaVersion3),
  scalaVersion := scalaVersionInBuild,
  Test / parallelExecution := false,
  scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) =>
      Seq(
        "-deprecation", // Emit warning and location for usages of deprecated APIs.
        "-encoding",
        "utf-8", // Specify character encoding used by source files.
        "-feature", // Emit warning and location for usages of features that should be imported explicitly.
        "-language:postfixOps",
        "-no-indent",
        "-explain",
        "-experimental", // needed for some macros
        "-unchecked", // Enable additional warnings where generated code depends on assumptions.
        "-Xfatal-warnings" // Fail the compilation if there are any warnings.
      )
    case Some((2, _)) =>
      Seq(
        "-opt:l:method",
        "-opt:l:inline",
        "-opt-inline-from:org.saddle.**",
        "-opt-warnings",
        "-Wopt",
        "-deprecation", // Emit warning and location for usages of deprecated APIs.
        "-encoding",
        "utf-8", // Specify character encoding used by source files.
        "-feature", // Emit warning and location for usages of features that should be imported explicitly.
        "-language:postfixOps",
        "-language:existentials",
        "-unchecked", // Enable additional warnings where generated code depends on assumptions.
        // "-Xfatal-warnings", // Fail the compilation if there are any warnings.
        "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
        "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
        "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
        "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
        "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
        "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
        "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
        "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
        "-Xlint:option-implicit", // Option.apply used implicit view.
        "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
        "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
        "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
        "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
        "-Ywarn-dead-code", // Warn when dead code is identified.
        // "-Ywarn-numeric-widen", // Warn when numerics are widened.
        "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
        "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
        "-Ywarn-unused:locals", // Warn if a local definition is unused.
        "-Ywarn-unused:params", // Warn if a value parameter is unused.
        "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
        "-Ywarn-unused:privates" // Warn if a private member is unused.
      )
    case _ => ???
  }),
  Compile / console / scalacOptions ~= (_ filterNot (_ == "-Xfatal-warnings"))
) ++ Seq(
  organization := "io.github.pityka",
  licenses += ("MIT", url("https://opensource.org/licenses/MIT")),
  Global / cancelable := true,
  fork := true,
  javaOptions += "-Xmx4G"
)

lazy val akkaVersion = "2.6.19"

lazy val akkaProvided = List(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion % Provided,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Provided,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion % Provided
)

lazy val specs = List(
  ("org.specs2" %% "specs2-core" % "4.14.1-cross" % "test"),
  ("org.specs2" %% "specs2-scalacheck" % "4.14.1-cross" % "test")
)

lazy val root = project
  .in(file("."))
  .settings(commonSettings: _*)
  .settings(
    publishArtifact := false,
    publish / skip := true
  )
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(commonSettings: _*)
  .settings(
    name := "ra3-core",
    libraryDependencies ++= List(
      "io.airlift" % "aircompressor" % "0.25",
      "org.scalameta" %% "munit" % "1.0.0-M10" % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "io.github.pityka" %% "saddle-core" % "4.0.0-M11",
      "io.github.pityka" %% "tasks-core" % "3.0.0-M15",
      "de.lhns" %% "fs2-compress-gzip" % "1.0.0",
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.30.13" % "compile-internal"
    ) ++ akkaProvided ++ specs
  )


