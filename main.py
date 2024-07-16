from quixstreams import Application
import uuid

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

# Connect to the Quix public broker to consume data
app = Application(
    broker_address="publickafka.quix.io:9092",  # Kafka broker address
    consumer_group=str(uuid.uuid4()),  # Kafka consumer group
    auto_offset_reset="latest",  # Read topic from the end
    producer_extra_config={"enable.idempotence": False},
    use_changelog_topics=False
)

# set the topic name. In this case the topc that's available on the public Kafka.
input_topic = app.topic("demo-onboarding-prod-chat", value_deserializer="json")

sdf = app.dataframe(input_topic)

sdf["tokens_count"] = sdf["message"].apply(lambda message: len(message.split(" ")))
sdf = sdf[["role", "tokens_count"]]

# function accepts data as dict
def func(data: dict):
    
    # Here is where you do whatever you want with your data.
    # - sink to storage 
    # - publish to a 3rd party
    # - create a Slack alert

    print(data)
    pass
    
# process the data in a lambda or function
sdf = sdf.update(func)

if __name__ == "__main__":
    app.run(sdf)