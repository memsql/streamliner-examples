resolvers += "memsql-internal" at "http://coreos-10.memcompute.com:8080/repository/internal"

lazy val commonSettings = Seq(
  organization := "com.memsql",
  version := "0.0.1",
  scalaVersion := "2.10.5"
)

lazy val thrift = (project in file("thrift")).
  settings(commonSettings: _*).
  settings(
    name := "memsql-spark-streamliner-thrift-examples",
    parallelExecution in Test := false,
    test in assembly := {},
    libraryDependencies ++= {
      Seq(
        "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
        "org.apache.spark" %% "spark-streaming" % "1.4.1" % "provided",
        "org.apache.spark" %% "spark-sql" % "1.4.1"  % "provided",
        "org.apache.thrift" % "libthrift" % "0.9.2",
        "com.memsql" %% "memsqletl" % "0.1.9" % "provided",
        "org.scalatest" %% "scalatest" % "2.2.5" % "test"
      )
    }
  )

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "memsql-spark-streamliner-examples",
    libraryDependencies  ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
        "org.apache.spark" %% "spark-sql" % "1.4.1"  % "provided",
        "org.apache.spark" %% "spark-streaming" % "1.4.1" % "provided",
        "com.memsql" %% "memsqletl" % "0.1.9" % "provided"
    )
)
