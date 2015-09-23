MemSQL Spark Streamliner Examples
=================================

This is a repository featuring example code for the [MemSQL Spark Streamliner](http://docs.memsql.com/latest/spark/).

MemSQL Spark Streamliner lets you build custom Spark pipelines to:
   1. extract from real-time data sources such as Kafka,
   2. transform data structures such as CSV, JSON, or Thrift in table rows,
   3. load your data into MemSQL.

Check out:

   - [Examples of Extractors](./src/main/scala/com/memsql/streamliner/examples/Extractors.scala)
   - [Examples of Transformers](./src/main/scala/com/memsql/streamliner/examples/Transformers.scala)
   - ... and browse the code for more


Get Started with MemSQL Spark Streamliner
-----------------------------------------

Check out the [MemSQL Spark Streamliner Starter](https://github.com/memsql/streamliner-starter) repository.

Or read more on how to [create custom Spark Interface JARs](http://docs.memsql.com/latest/spark/memsql-spark-interface/) in our docs.


Contribute to MemSQL Spark Streamliner Examples
-----------------------------------------------

Please submit a pull request with new Extractors and Transformers.

When you contribute code, you affirm that the contribution is your original work and that you license the work to the project under the project's open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project's open source license and warrant that you have the legal authority to do so.


Build the Examples
------------------

Clone the repository, then run:

```bash
make build
```

The JAR will be placed in `target/scala-<version>/`. You can upload the JAR to MemSQL Ops and create a pipeline using this or your custom code.


Run Tests
---------

Run:

```bash
make test
```
