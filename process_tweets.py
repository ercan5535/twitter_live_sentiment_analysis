import re

from textblob import TextBlob
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType

def CleanTweet(tweet):
    tweet = re.sub(r"@[A-Za-z0-9]", "", tweet) # Remove @mentions
    tweet = re.sub(r"#", "", tweet) # Remove '#' symbol
    tweet = re.sub(r"RT[\s]+", "", tweet) #Remove RT
    tweet = re.sub(r"https?:\/\/\S+", "", tweet) # Remove the hyper link
    tweet = re.sub(r"[\t\n\r]+", "", tweet) # Remove space symbols

    return tweet

def AnalyzePolarity(polarity):
    if polarity < 0:
        return 'NEGATIVE'
    elif polarity == 0:
        return 'NEUTRAL'
    else:
        return 'POSITIVE'


# Create Schema
schema = StructType([
    StructField("tweet_text", StringType(), True),
    StructField("tweet_url", StringType(), True)
])

# Create Spark Session
spark = SparkSession.builder.appName("TweetSentimentAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read data from Kafka topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
  .option("subscribe", "twitter") \
  .option("startingOffsets", "earliest") \
  .load()

df = df.selectExpr("CAST(value AS STRING)")

# Parse columns from Json
df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
# +--------------------+--------------------+
# |          tweet_text|           tweet_url|
# +--------------------+--------------------+
# |#Technology #Heal...|https://twitter.c...|
# |@rea_reatha @Magg...|https://twitter.c...|
# |RT @narendramodi:...|https://twitter.c...|
# |RT @EUhealt: Does...|https://twitter.c...|
# |RT @County19Nyeri...|https://twitter.c...|

# Clean Tweets
udf_clean_tweets = udf(CleanTweet, StringType())
df = df.withColumn("clean_tweet", udf_clean_tweets("tweet_text"))

# Sentiment analysis
polarity_detection_udf = udf(lambda x: TextBlob(x).sentiment.polarity, 
                                StringType())
subjectivity_detection_udf = udf(lambda x: TextBlob(x).sentiment.subjectivity,
                                StringType())
analyze_polarity = udf(AnalyzePolarity, StringType())

df = df.withColumn("polarity", polarity_detection_udf("clean_tweet"))
df = df.withColumn("subjectivity", subjectivity_detection_udf("clean_tweet"))
df = df.withColumn("analysis", analyze_polarity("polarity"))

# +--------------------+--------------------+--------------------+--------------------+-------------------+--------+
# |          tweet_text|           tweet_url|         clean_tweet|            polarity|       subjectivity|analysis|
# +--------------------+--------------------+--------------------+--------------------+-------------------+--------+
# |#Technology #Heal...|https://twitter.c...|Technology Health...| 0.26266233766233765| 0.6243506493506493|POSITIVE|
# |@rea_reatha @Magg...|https://twitter.c...|ea_reatha aggieEa...|                 0.5|                0.5|POSITIVE|
# |RT @narendramodi:...|https://twitter.c...|arendramodi: Glad...|                 0.5|                1.0|POSITIVE|
# |RT @EUhealt: Does...|https://twitter.c...|Uhealt: Does The ...|                 0.0|                0.0| NEUTRAL|
# |RT @County19Nyeri...|https://twitter.c...|ounty19Nyeri: H.E...|                 0.0|                0.0| NEUTRAL|

query = \
    df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()