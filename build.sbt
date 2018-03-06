name := "MachbaseSlickDriverTest"
version := "1.0.0"
scalaVersion := "2.12.4"
 
unmanagedJars in Compile += file("lib/machbase.jar")

libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick" % "3.2.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.2.1",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "com.zaxxer" % "HikariCP" % "2.5.1"
)
