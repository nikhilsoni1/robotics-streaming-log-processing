import pulsar
import io
import sys
from mcap_protobuf.decoder import DecoderFactory
from mcap.reader import make_reader
import files.schema_exec_pb2 as schema_exec_pb2
import files.schema_sensor_health_pb2 as schema_sensor_health_pb2

def main():
    with open(sys.argv[1], "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for schema, channel, message, proto_msg in reader.iter_decoded_messages():
            print(f"{channel.topic} {schema.name} [{message.log_time}]: {proto_msg}")



client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('my-topic', subscription_name='my-sub')

while True:
    msg = consumer.receive()
    # print("Received message: '%s'" % msg.data())
    try:
        # Extract MCAP binary data from the message
        mcap_data = msg.data()
        with io.BytesIO(mcap_data) as mcap_stream:
            reader = make_reader(mcap_stream, decoder_factories=[DecoderFactory()])
            for schema, channel, message, proto_msg in reader.iter_decoded_messages():
                if channel.topic == "topic/app_exec":
                    print(f">> num_apps:{proto_msg.num_apps} <<")
        consumer.acknowledge(msg)
    except Exception as e:
        print("Error processing message:", e)
        # consumer.negative_acknowledge(msg)
client.close()