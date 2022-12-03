# Apache Flink AWS Connectors end-to-end tests

To run the end-to-end tests you will need a `flink-dist`. You can build Flink from source or download 
from https://dist.apache.org/repos/dist/release/flink. For example, download
[flink-1.16.0-bin-scala_2.12.tgz](https://dist.apache.org/repos/dist/release/flink/flink-1.16.0/flink-1.16.0-bin-scala_2.12.tgz)
and extract, then find `flink-dist-1.16.0.jar` in the `lib` folder.

The end-to-end tests are disabled by default, to run them you can use the `run-end-to-end-tests` maven profile.

Example command to run end-to-end tests:
```
mvn clean verify -Prun-end-to-end-tests -DdistDir=<path-to-dist>/flink-1.16.0/lib/flink-dist-1.16.0.jar 
```

