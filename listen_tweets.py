import json
import creds_confs
import tweepy
from kafka import KafkaProducer

class TweetListener(tweepy.StreamingClient):
    def __init__(self, bearer_token):
        super().__init__(bearer_token)

        # Define Kafka Producer
        self.producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                    value_serializer=lambda x: json.dumps(x).encode('ascii'))
        self.topic = creds_confs.kafka_topic_name

    def on_data(self, data):
        try:
            json_data = json.loads(data)
            print(json_data)

            # Create data to send
            send_data = {
                "tweet_text": json_data["data"]["text"],
                "tweet_url": r"https://twitter.com/twitter/statuses/" + str(json_data["data"]["id"])
            }

            # Send data to Kafka topic
            self.producer.send(self.topic, send_data)
            return True

        except Exception as e:
            print("Error on_data: %s" % str(e))
            return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == "__main__":

    # Get credentials
    bearer_token = creds_confs.bearer_token

    # Start Streaming
    twitter_stream = TweetListener(bearer_token=bearer_token)
    twitter_stream.add_rules(tweepy.StreamRule(creds_confs.tweet_keyword))
    twitter_stream.filter()

