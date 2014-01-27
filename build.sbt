
name := "matchmaker-spark-process"

version := "1.0.0"
 
scalaVersion := "2.10.3"

externalPom()

resolvers += (
    "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

scalacOptions := Seq("-deprecation", "-unchecked")

