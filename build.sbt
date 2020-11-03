name := "mongo-experiment"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "4.1.1"
libraryDependencies += "org.typelevel" %% "cats-effect" % "2.2.0"

mainClass in (Compile, run) := Some("MongoExperimentApp")
