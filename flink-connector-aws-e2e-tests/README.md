# Apache Flink AWS Connectors end-to-end tests

To run the end-to-end tests you will need a `flink-dist`. You can build Flink from source or download 
from https://dist.apache.org/repos/dist/release/flink. For example, download
`flink-{FLINK_VERSION}-bin-scala_{SCALA_VERSION}.tgz` and extract, then find `flink-dist-{FLINK_VERSION}.jar` in the `lib` folder.

The end-to-end tests are disabled by default, to run them you can use the `run-end-to-end-tests` maven profile.

Example command to run end-to-end tests:
```
mvn clean verify -Prun-end-to-end-tests -DdistDir=<path-to-dist>/flink-{FLINK_VERSION}/lib/flink-dist-{FLINK_VERSION}.jar 
```

