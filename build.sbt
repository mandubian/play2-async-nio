name := "play2-async-nio"

organization := "play2.tools.nio"

version := "0.1-SNAPSHOT"

scalaVersion := "2.9.2"

resolvers ++= Seq(
  "mandubian-mvn snapshots" at "https://github.com/mandubian/mandubian-mvn/raw/master/snapshots",
  "mandubian-mvn releases" at "https://github.com/mandubian/mandubian-mvn/raw/master/releases",
  "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots",
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases"
)

libraryDependencies ++= Seq(
  "play" %% "play" % "2.1-SNAPSHOT",
  "net.java.dev.jna"  % "jna" % "3.2.2",
  "play" %% "play-test" % "2.1-SNAPSHOT" % "test",
  "org.specs2" %% "specs2" % "1.7.1" % "test",
  "junit" % "junit" % "4.8" % "test"  
)

publishTo <<=  version { (v: String) => 
    val base = "../../workspace_mandubian/mandubian-mvn"
	if (v.trim.endsWith("SNAPSHOT")) 
		Some(Resolver.file("snapshots", new File(base + "/snapshots")))
	else Some(Resolver.file("releases", new File(base + "/releases")))
}

publishMavenStyle := true

publishArtifact in Test := false