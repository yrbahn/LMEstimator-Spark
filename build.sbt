lazy val root = (project in file(".")).
  settings(
    name := "sparklm",
    version := "0.1",
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("sparklm.SparkLM") 
  )

val sparkVersion = "2.0.1"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  "com.typesafe" % "config" % "1.2.1",
  //"com.kakao" % "dha" % "2.7.7"
  "it.unimi.dsi" % "fastutil" % "7.0.13",
  "org.clapper" % "argot_2.11" % "1.0.3",
  "com.twitter.penguin" % "korean-text" % "4.4"
)

scalacOptions ++= Seq("-encoding", "UTF-8")
