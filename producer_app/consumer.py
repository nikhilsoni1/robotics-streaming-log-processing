import pulsar
import io
# import sys
from mcap_protobuf.decoder import DecoderFactory
from mcap.reader import make_reader
# import files.schema_exec_pb2 as schema_exec_pb2
# import files.schema_sensor_health_pb2 as schema_sensor_health_pb2
import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime
from uuid import uuid4
import os
import time
def protobuf_to_parquet(proto_msgs, file_path):
    """Converts a list of Protobuf messages into a flattened Parquet format using PyArrow."""
    records = []
    last_time = time.monotonic()
    for msg in proto_msgs:
        msg_dict = MessageToDict(msg)
        ts = datetime.fromtimestamp(msg.timestamp.seconds)  # Convert Protobuf Timestamp
        num_apps = msg_dict.get("numApps", 0)

        for app in msg_dict.get("apps", []):
            records.append({
                "ts": ts,
                "num_apps": num_apps,
                "app_name": app["appName"],
                "exec_time": app.get("execTime", None),
                "cpu_usage": app.get("cpuUsage", None),
            })
        if time.monotonic() - last_time > 10:
            print(f"Processed {len(records)} records so far...")
            last_time = time.monotonic()
    fpath = os.path.join(file_path, f"{uuid4()}.parquet")
    # Convert to PyArrow Table
    table = pa.Table.from_pylist(records)
    # Write to Parquet
    pq.write_table(table, fpath)
    return fpath



client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('my-topic', subscription_name='my-sub')
last_time = time.monotonic()
msgs = list()
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
                    msgs.append(proto_msg)
            
                if time.monotonic() - last_time > 10:
                    print("Processed 10 seconds worth of data")
                    protobuf_to_parquet(msgs, "./files/json_store/")
                    msgs = list()
                    last_time = time.monotonic()
                else:
                    pass
        consumer.acknowledge(msg)
    except Exception as e:
        print(f"Error processing message: {e}")
        # consumer.negative_acknowledge(msg)
client.close()