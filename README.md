#consumer-dispatcher
This sample contains kafka producer, kafka consumer, dispatcher and worker(s).

- kafka producer : produces messages and then sends these messages to kafka topic.
- kafka consumer : consumes messages from kafka topic and then sends these messages to dispatcher job queue channel.
- dispatcher : dispatcher has job queue channel and has worker pool. Dispatcher sends jobs to registered worker's job channels.
- worker : worker has job channel, registers themselves to dispatcher worker pool and processes messages coming from their job channel.  
