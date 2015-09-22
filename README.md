MemSQL Spark Streamliner Examples
=================================

This is a repository featuring example code for the [MemSQL Spark Streamliner](http://docs.memsql.com/latest/spark/).

MemSQL Spark Streamliner lets you build custom Spark pipelines to:
   1. extract from real-time data sources such as Kafka,
   1. transform data structures such as CSV, JSON, or Thrift in table rows,
   1. load your data into MemSQL.

Check out:

   - [Examples of Extractors](./src/main/scala/com/memsql/streamliner/examples/Extractors.scala)
   - [Examples of Transformers](./src/main/scala/com/memsql/streamliner/examples/Transformers.scala)
   - ... and browse the code for more


Get Started with MemSQL Spark Streamliner
-----------------------------------------

Check out the [MemSQL Spark Streamliner Starter](https://github.com/memsql/streamliner-starter) repository.

Or read more on how to [create custom Spark Interface JARs](http://docs.memsql.com/latest/spark/memsql-spark-interface/) in our docs.


Build the Examples
------------------

Clone the repository, then run:

```bash
make build
```
