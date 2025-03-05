lazy val root = (project in file(".")).
  settings(
    name := "mozartGame",
    version := "1.0",
    scalaVersion := "2.12.12"
  )
  

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.18"
libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.5.18"


