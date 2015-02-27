organization := "com.github.tomdom"

name := """dbinsight"""

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.5"

crossScalaVersions := Seq("2.10.4", "2.11.5")

resolvers += ("tomdom-mvn snapshots" at "https://github.com/tomdom/tomdom-mvn/raw/master/snapshots")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.mockito" % "mockito-all" % "1.10.19",
  "com.github.tomdom" %% "scalabase" % "0.2-SNAPSHOT" changing()
)

publishTo := {
  val tomdomMvn = Path.userHome.absolutePath + "/projects/github/tomdom/tomdom-mvn"
  if (isSnapshot.value)
    Some(Resolver.file("file",  new File(tomdomMvn + "/snapshots")))
  else
    Some(Resolver.file("file",  new File(tomdomMvn + "/releases")))
}

scalariformSettings
