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
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from mcap.exceptions import RecordLengthLimitExceeded, EndOfFile


def protobuf_app_exec_to_line_protocol(msg):
    """Converts a Protobuf message into a flattened dictionary format."""
    msg_dict = MessageToDict(msg)
    ts = int(
        msg.timestamp.seconds * 1e9 + msg.timestamp.nanos
    )  # Convert Protobuf Timestamp to nanoseconds and safely typecast to int
    num_apps = msg_dict.get("numApps", 0)
    list_of_points = list()
    for app in msg_dict.get("apps", []):
        point = (
            Point("app_exec")
            .tag("app_name", app.get("appName"))
            .tag("num_apps", num_apps)
            .field("exec_time", app.get("execTime", None))
            .field("cpu_usage", app.get("cpuUsage", None))
            .time(ts, WritePrecision.NS)
        )
        list_of_points.append(point)
    return list_of_points

def protobuf_sensor_health_to_line_protocol(msg):
    """Converts a Protobuf message into a flattened dictionary format."""

    msg_dict = MessageToDict(msg)
    ts = int(msg.timestamp.seconds * 1e9 + msg.timestamp.nanos)
    num_sensors = msg_dict.get("numSensors", 0)
    list_of_points = list()
    for idx, i in enumerate(msg_dict.get("sensorTemps", [])):
        point = (
            Point("sensor_health")
            .tag("num_sensors", num_sensors)
            .tag("sensor_id", idx)
            .field("sensor_temp", i)
            .time(ts, WritePrecision.NS)
        )
        list_of_points.append(point)
    return list_of_points

def protobuf_to_parquet(proto_msgs, file_path):
    """Converts a list of Protobuf messages into a flattened Parquet format using PyArrow."""
    records = []
    for msg in proto_msgs:
        msg_dict = MessageToDict(msg)
        ts = (
            msg.timestamp.seconds * 1e9 + msg.timestamp.nanos
        )  # Convert Protobuf Timestamp to nanoseconds
        num_apps = msg_dict.get("numApps", 0)
        for app in msg_dict.get("apps", []):
            records.append(
                {
                    "ts": ts,
                    "num_apps": num_apps,
                    "app_name": app["appName"],
                    "exec_time": app.get("execTime", None),
                    "cpu_usage": app.get("cpuUsage", None),
                }
            )

    fpath = os.path.join(file_path, f"{uuid4()}.parquet")
    table = pa.Table.from_pylist(records)
    pq.write_table(table, fpath)
    return fpath

def consumer():
    pulsar_url = "pulsar://localhost:6650"
    topic = "my-topic"
    subscription_name = "my-sub"
    influx_url = "http://localhost:8086"
    influx_org = "myorg"
    influx_bucket = "bucket01"
    influx_token = "x8ZENWA2tU4tJ3a-OWIoPWlFk5lmKppq80qLhLnDk64H6RPTewubkgX13eArWwLuYf4JREeFcZQg27aedU18_g=="


    # Initialize Pulsar client and consumer
    client = pulsar.Client(pulsar_url)
    consumer = client.subscribe(topic, subscription_name=subscription_name, consumer_type=pulsar.ConsumerType.Shared)

    # Initialize InfluxDB client and write API
    influx_client = influxdb_client.InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    write_api = influx_client.write_api()
    while True:
        msg = consumer.receive()
        points_list = list()
        mcap_data = msg.data()
        mcap_stream = io.BytesIO(mcap_data)
        try:
            reader = make_reader(mcap_stream, decoder_factories=[DecoderFactory()])
            for schema, channel, message, proto_msg in reader.iter_decoded_messages():
                if channel.topic == "topic/app_exec":
                    points = protobuf_app_exec_to_line_protocol(proto_msg)
                    points_list += points
                elif channel.topic == "topic/sensor_health":
                    points = protobuf_sensor_health_to_line_protocol(proto_msg)
                    points_list += points
                else:
                    pass
            consumer.acknowledge(msg)
        except (RecordLengthLimitExceeded, EndOfFile) as e:
            print(f"Listening....")
            mcap_stream.close()
        
        for p in points_list:
            write_api.write(bucket=influx_bucket, org=influx_org, record=p)
            print("Data written successfully!")
        mcap_stream.close()

# Launch multiple consumer threads
threads = []
num_consumers = 3  # Adjust this to the number of consumers you need

for _ in range(num_consumers):
    # t = threading.Thread(target=consume_messages)
    t = threading.Thread(target=consumer)
    t.start()
    threads.append(t)

for t in threads:
    t.join()
