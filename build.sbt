lazy val commonSettings = Seq(
  organization := "com.memsql",
  version := "0.0.1",
  scalaVersion := "2.10.5"
)

lazy val avro = (project in file("avro")).
  settings(commonSettings: _*).
  settings(
    name := "memsql-spark-streamliner-avro-examples",
    parallelExecution in Test := false,
    test in assembly := {},
    libraryDependencies ++= {
      Seq(
        "org.apache.spark" %% "spark-core" % "1.5.1" % "provided",
        "org.apache.spark" %% "spark-streaming" % "1.5.1" % "provided",
        "org.apache.spark" %% "spark-sql" % "1.5.1"  % "provided",
        "org.apache.avro" % "avro" % "1.7.7",
        "org.scalatest" %% "scalatest" % "2.2.5" % "test",
        "com.memsql" %% "memsql-etl" % "1.1.0"
      )
    }
  )

lazy val thrift = (project in file("thrift")).
  settings(commonSettings: _*).
  settings(
    name := "memsql-spark-streamliner-thrift-examples",
    parallelExecution in Test := false,
    test in assembly := {},
    libraryDependencies ++= {
      Seq(
        "org.apache.spark" %% "spark-core" % "1.5.1" % "provided",
        "org.apache.spark" %% "spark-streaming" % "1.5.1" % "provided",
        "org.apache.spark" %% "spark-sql" % "1.5.1"  % "provided",
        "org.apache.thrift" % "libthrift" % "0.9.2",
        "org.scalatest" %% "scalatest" % "2.2.5" % "test",
        "com.memsql" %% "memsql-etl" % "1.1.0"
      )
    }
  )

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "memsql-spark-streamliner-examples",
    parallelExecution in Test := false,
    test in assembly := {},
    libraryDependencies  ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.5.1" % "provided",
        "org.apache.spark" %% "spark-sql" % "1.5.1"  % "provided",
        "org.apache.spark" %% "spark-streaming" % "1.5.1" % "provided",
        "org.scalatest" %% "scalatest" % "2.2.5" % "test",
        "com.memsql" %% "memsql-etl" % "1.1.0"
    )
)
