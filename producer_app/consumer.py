import pulsar
import io
import pyarrow as pa
import pyarrow.parquet as pq
from mcap_protobuf.decoder import DecoderFactory
from mcap.reader import make_reader
from google.protobuf.json_format import MessageToDict
from datetime import datetime
from uuid import uuid4
import os
import time
import threading

def protobuf_to_parquet(proto_msgs, file_path):
    """Converts a list of Protobuf messages into a flattened Parquet format using PyArrow."""
    records = []
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

    fpath = os.path.join(file_path, f"{uuid4()}.parquet")
    table = pa.Table.from_pylist(records)
    pq.write_table(table, fpath)
    return fpath

def consume_messages():
    client = pulsar.Client('pulsar://localhost:6650')
    consumer = client.subscribe('my-topic', subscription_name='my-sub', consumer_type=pulsar.ConsumerType.Shared)
    # last_time = time.monotonic()
    # msgs = []
    thread_id = threading.get_ident()
    while True:
        msg = consumer.receive()
        msgs = list()
        try:
            mcap_data = msg.data()
            with io.BytesIO(mcap_data) as mcap_stream:
                reader = make_reader(mcap_stream, decoder_factories=[DecoderFactory()])
                for schema, channel, message, proto_msg in reader.iter_decoded_messages():
                    if channel.topic == "topic/app_exec":
                        msgs.append(proto_msg)
                        fpath = protobuf_to_parquet([proto_msg], "./files/json_store")
                        print(f"Thread ID: {thread_id} - {fpath}")
            consumer.acknowledge(msg)
        except Exception as e:
            print(f"Error processing message: {e}")
    
    client.close()

# Launch multiple consumer threads
threads = []
num_consumers = 3  # Adjust this to the number of consumers you need

for _ in range(num_consumers):
    t = threading.Thread(target=consume_messages)
    t.start()
    threads.append(t)

for t in threads:
    t.join()
