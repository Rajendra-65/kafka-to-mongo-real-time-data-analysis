from datetime import datetime
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd
import time

def delivery_report(err,msg):
    if err is not None:
        print("Delivery failed for User record {}:{}".format(msg.key(),err))
        return
    print('user record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(),msg.topic(),msg.partition(),msg.offset
    ))

kafka_config = {
    'bootstrap.servers':'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms':'PLAIN',
    'security.protocol':'SASL_SSL',
    'sasl.username':'L7MUKOBFABR7RB2E',
    'sasl.password':'7yFjiKGr3mXY3MRC5KoyRfRb3npxH55nvRESqjoZpliBBJZrG0UV1zRwLgOgoB3d'
}

schema_registry_client = SchemaRegistryClient({
    'url':'https://psrc-lzvd0.ap-southeast-2.aws.confluent.cloud',
    'basic.auth.user.info':'{}:{}'.format
    ('7Y5JR4JDXR4DJMVH','ueQ3dPMKrqpZP0EoaLmST5pk/KWqlR1bd3px4fsZkbhBRC+if57/3BSU931Coymu')
})

subject_name = 'logistics-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client,schema_str)

producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer' : avro_serializer
})

df = pd.read_csv('shipment_data.csv')
df = df.fillna('null')

for index,row in df.iterrows():
    value = row.to_dict()
    producer.produce(topic='logistics', key=str(index), value=value, on_delivery=delivery_report)
    producer.flush()
    time.sleep(1)

print("successfully published to the kafka topic")