Some Desing considerations when launching Spak apps:

# 1 Parametrizing
Pass the spark master name as an app arg as per below:

https://spark.apache.org/docs/2.1.0/submitting-applications.html

./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]

# 2 Execution threads / workers
Set thw number of execution threads fixed with local[K]	OR with as many worker threads as logical cores with local[*].
To receive multiple streams of data in parallel create multiple input DStreams but note that a Spark worker/executor is a long-running task, hence it occupies one of the cores allocated to the Spark Streaming application.
A Spark Streaming application needs to be allocated enough cores (or threads, if running locally).

Alternatively, the spark-submit script can load default Spark configuration values file conf/spark-defaults in the Spark directory to obviate the need for certain flags to spark-submit.

The batch interval must be set based on the latency requirements of your application and available cluster resources. To verify whether the system is able to keep up with the data rate, you can check the value of the end-to-end delay experienced by each processed batch (either look for “Total delay” in Spark driver log4j logs, or use the StreamingListener interface). If the delay is maintained to be comparable to the batch size, then system is stable. Otherwise, if the delay is continuously increasing, it means that the system is unable to keep up and it therefore unstable.

# 3 Periodic RDD checkpointing
Metadata checkpointing is primarily needed for recovery from driver failures, whereas data or RDD checkpointing is necessary even for basic functioning if stateful transformations are used (e.g.: updateStateByKey or reduceByKeyAndWindow).
Data checkpointing can be configured via enabling "write ahead logs" to ensure all the data received from a receiver gets written into a write ahead log in the configuration checkpoint directory to prevent data loss on driver recovery.
The checkpoint directory must be provided to allow for periodic RDD checkpointing when using stateful transformations  or when to recover from failures of the driver running the application.

# 4 Automatic restart
To automatically recover from a driver failure, the deployment infrastructure that is used to run the streaming application must monitor the driver process and relaunch the driver if it fails. A way to do this is by running the application driver itself on one of the worker nodes and instruct the cluster manager to supervise the driver and relaunch it if the driver fails either due to non-zero exit code, or due to failure of the node running the driver.

# 4.a Graceful Shutdown
Using StreamingContext.stop(...) or JavaStreamingContext.stop(...) to ensure data that has been received is completely processed before shutdown.

# 5 Monitoring
Besides the standard Spark monitorind (https://spark.apache.org/docs/2.1.0/monitoring.html), the following two Straming metrics are particularly important:
Processing Time - The time to process each batch of data.
Scheduling Delay - the time a batch waits in a queue for the processing of previous batches to finish.

If the batch processing time is consistently more than the batch interval and/or the queueing delay keeps increasing, then it indicates that the system is not able to process the batches as fast they are being generated and is falling behind. In that case, consider reducing the batch processing time.
The progress of a Spark Streaming program can also be monitored using the StreamingListener interface,.

# 6 Kafka specific:
See param PreferConsistent for evenly distributed executors across partitions 
It is possible to create an RDD from a defined range of offsets using: KafkaUtils.createRDD
Kafka support SSL with secured user/pass in keystore with param: kafkaParams.put("security.protocol", "SSL")

Pending: how to achieve exactly once semantics (3 strategies)
https://spark.apache.org/docs/2.1.0/streaming-kafka-0-10-integration.html

# 7 Failure Analysis

There are two kinds of failures that we should be concerned about:

Failure of a Worker Node - Any of the worker nodes running executors can fail, and all in-memory data on those nodes will be lost. If any receivers were running on failed nodes, then their buffered data will be lost.

Failure of the Driver Node - If the driver node running the Spark Streaming application fails, then obviously the SparkContext is lost, and all executors with their in-memory data are lost.

If any partition of an RDD is lost due to a worker node failure, then that partition can be re-computed from the original fault-tolerant dataset using the lineage of operations.

Pending to address these from: https://spark.apache.org/docs/2.1.0/streaming-programming-guide.html

Not covered: Memory Tuning

Covered elsewhere: dependencies https://github.com/jacace/sample_tech_spark_java_streaming/issues/1