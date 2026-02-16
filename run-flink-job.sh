mvn -q -DskipTests clean package
netstat -an | grep 8081 | grep LISTEN
export PATH=$PATH:/workspaces/flink-demo/flink-2.0.0/bin
#flink run -c com.globomantics.UniqueAdClickAggregator target/flink-unique-clicks-1.0.0-shaded.jar out
flink run -c com.globomantics.UniqueAdClickAggregator target/flink-unique-clicks-1.0.0.jar out

#ONA:
# https://app.gitpod.io/details/019c6334-fa1a-7f14-893c-a759409d352d

#Flink UI:
# https://8081--019c6334-fa1a-7f14-893c-a759409d352d.us-east-1-01.gitpod.dev/#/overview


