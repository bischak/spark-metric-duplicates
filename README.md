## Metric duplicates analytic application

Application collects records with metrics and makes duplicates calculation for certain date.
Application runs on Spark cluster, uses Apache Cassandra distributed NoSQL database and uses Apache Kafka message broker as input source.

The solution answers for the question: How many records are duplicated by some metric for certain date.

### Terms

* **Record** is one particular source of interest.
For example: unique visitor ID in [Web analytics](https://en.wikipedia.org/wiki/Web_analytics), device ID in [IoC](https://en.wikipedia.org/wiki/Internet_of_things), user ID in [E-commerce](https://en.wikipedia.org/wiki/E-commerce), [Finance](https://en.wikipedia.org/wiki/Finance) or [Insurance](https://en.wikipedia.org/wiki/Insurance).
 
* **Metric** is a particular property of record.

* **Duplicate** calculation is an example of aggregation analytic on records data. **Duplicate** is count of records that repeated for particular metric for particular time range. 


### Apps

The solution consists of two Spark applications: **MetricsCollectorApp** and **MetricsAggregatorApp**.
The **MetricsCollectorApp** reads Kafka topic, stores records in Cassandra.

The **MetricsAggregatorApp** aggregates all metric duplicates for all records and stores results in Cassandra.

The **MetricsCollectorApp** is a Spark Streaming application and the **MetricsAggregatorApp** is a daily Spark Batch Job application.  
 

### Input

The **MetricsCollectorApp* listens certain Kafka topic.

A message in the topic has the following format:

    {"record":"e55c31ae-b965-41a7-a4c3-b8c4178dedd3","metric":"CATEGORY2","time":1493138491461}
    {"record":"e55c31ae-b965-41a7-a4c3-b8c4178dedd3","metric":"CATEGORY1","time":1493136538770}
    
The field "time" has Unix time format. We consider that all time values is in UTC.
    
### Output

The **MetricsCollectorApp** stores result duplicates analytic in Cassandra table **duplicates**:

    CREATE TABLE metrics.duplicates (
        day timestamp,
        metric text,
        dup int,
        PRIMARY KEY (day, metric)
    )

It is possible to express calculation logic in SQL (e.g. PostgreSQL) using the following syntax:

    SELECT metric, sum(dup) AS duplicates
    FROM  (
       SELECT record, metric, count(*) - 1 AS dup
       FROM records
       WHERE time < 1494524204896
       GROUP BY record, metric
       HAVING count(*) > 1
    ) dp
    GROUP BY metric
    ORDER BY duplicates DESC;
    
Where 1494524204896 is timestamp of upper time border (excluding).   


### Run Kafka

Download Kafka https://kafka.apache.org/downloads

Run Zookeeper:

    bin/zookeeper-server-start.sh config/zookeeper.properties
    
    
Run Kafka server:

    bin/kafka-server-start.sh config/server.properties
    
    
Start producer:

    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic records
    
    
### Run apps from SBT

Run MetricsCollectorApp:

    run 'runMain app.MetricsCollectorApp'

Calculate all metrics for certain date:
    
    run 'runMain app.MetricsAggregatorApp 11.05.2017'
    
Calculate particular metric for certain date:
    
    run 'runMain app.MetricsAggregatorApp 11.05.2017 MY_METRIC_ID'
