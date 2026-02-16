mvn -q -DskipTests clean package
netstat -an | grep 8081 | grep LISTEN
flink run -c com.globomantics.UniqueAdClickAggregator target/flink-unique-clicks-1.0.0-shaded.jar out
