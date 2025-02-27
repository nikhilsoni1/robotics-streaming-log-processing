import time
import pulsar
import files.schema_exec_pb2 as schema_exec_pb2
import files.schema_sensor_health_pb2 as schema_sensor_health_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from mcap_protobuf.writer import Writer
import random
import io


client = pulsar.Client("pulsar://localhost:6650")
producer = client.create_producer("my-topic")


freq_to_ns = {
    "1Hz": 1_000_000_000,  # 1 second
    "10Hz": 100_000_000,  # 100 milliseconds
    "100Hz": 10_000_000,  # 10 milliseconds
    "1kHz": 1_000_000,  # 1 millisecond
}

selected_freq = "1Hz"
interval_ns = freq_to_ns[selected_freq]


def counter_generator():
    counter = 0
    while True:
        counter += 1
        yield counter


counter_gen = counter_generator()

_start_ns = time.time_ns()
_runtime_ns = 3600 * 1_000_000_000 # convert seconds to nanoseconds
while True:

    timestamp_ns = time.time_ns()

    # Create Timestamp in Protobuf Format
    timestamp = Timestamp()
    timestamp.seconds = timestamp_ns // 1_000_000_000
    timestamp.nanos = timestamp_ns % 1_000_000_000

    # Create Synthetic Data for the AppExec Message
    app1 = schema_exec_pb2.AppInfo(
        app_name="App1", exec_time=random.uniform(1, 3), cpu_usage=random.uniform(1, 3)
    )
    app2 = schema_exec_pb2.AppInfo(
        app_name="App2", exec_time=random.uniform(1, 3), cpu_usage=random.uniform(1, 3)
    )

    app_exec_message = schema_exec_pb2.AppExec(
        timestamp=timestamp, num_apps=2, apps=[app1, app2]
    )

    # Create Synthetic Data for the SensorHealth Message
    sensor_health_message = schema_sensor_health_pb2.SensorHealth(
        timestamp=timestamp,
        num_sensors=3,
        sensor_temps=[
            random.uniform(20, 40),
            random.uniform(20, 40),
            random.uniform(20, 40),
        ],
    )

    # Write the MCAP Log to an In-Memory Binary Stream
    mcap_stream = io.BytesIO()
    mcap_writer = Writer(mcap_stream)

    # make this 10hz
    mcap_writer.write_message(
        topic="topic/app_exec",
        message=app_exec_message,
        log_time=time.time_ns(),
        publish_time=time.time_ns(),
    )
    # make this 50hz
    mcap_writer.write_message(
        topic="topic/sensor_health",
        message=sensor_health_message,
        log_time=time.time_ns(),
        publish_time=time.time_ns(),
    )
    mcap_writer.finish()
    mcap_stream.seek(0)  # Reset the stream position before reading
    mcap_data = mcap_stream.read()
    producer.send(mcap_data)  # Send to Pulsar
    mcap_stream.close()
    # Send the Binary Data to Pulsar
    counter = next(counter_gen)  # Get the next counter value
    print(
        f"Published MCAP log {counter} at {timestamp.seconds}s, {timestamp.nanos}ns"
    )

    if time.time_ns() - _start_ns > _runtime_ns:
        break
    else:
        time.sleep(0.01)


client.close()
