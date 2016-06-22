# kafka-offset-reset
A simple tool to reset kafka offset

From kafka 0.9.0, group offset information is stored in kafka instead of zookeeper.
This tool is used to reset group offset to start point when needed.

# Usage

````
java -cp ".:lib/*" com.momo.live.kafka.OffsetReset kafka-server-host-port group-id topic partition-size
```