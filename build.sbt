
name := "matchmaker-spark-process"

version := "1.0.0"
 
scalaVersion := "2.10.3"

externalPom()

resolvers += Resolver.url("sbt-plugin-releases", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.7.4")

resolvers += (
    "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

scalacOptions := Seq("-deprecation", "-unchecked")

