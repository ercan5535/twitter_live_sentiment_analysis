# Twitter Stream Sentiment Analysis
In this project new tweets ,which are contains entered keyword, get sentiment analysis on streaming. <br>
Tweepy - Twitter API v2 Reference is used for get tweets as stream <br>
PySpark and TextBlob are used for sentiment analysis on streaming <br>


# Installation and Starting Services
## Step 1) Start Kafka broker service

#### - Get Kafka
$ wget https://dlcdn.apache.org/kafka/3.3.1/kafka_2.13-3.3.1.tgz <br>
$ tar -xzf kafka_2.13-3.3.1.tgz<br>
$ cd kafka_2.13-3.3.1<br>


#### - Start the Kafka environment
NOTE: Your local environment must have Java 8+ installed.
Run the following commands in order to start all services in the correct order:

- Start the ZooKeeper service <br>
$ bin/zookeeper-server-start.sh config/zookeeper.properties
- Start the Kafka broker service <br>
Open another terminal session and run:<br>
$ bin/kafka-server-start.sh config/server.properties

Once all services have successfully launched, you will have a basic Kafka environment running and ready to use. 

#### - Create topic:<br>
$ bin/kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092

## Step 2) Start Stream Listener
Open a new terminal session and run:<br>
$ python listen_tweets.py

## Step 3) Start PySpark
Open a new terminal session and run:<br>
$ spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 --jars elasticsearch-spark-30_2.12-8.5.3.jar consumer.py<br>
