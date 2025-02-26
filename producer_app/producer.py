import time
import pulsar
import files.schema_exec_pb2 as schema_exec_pb2  # Import the Protobuf schema
from google.protobuf.timestamp_pb2 import Timestamp
from mcap_protobuf.writer import Writer
import random
import io  # Import io for in-memory binary streams


client = pulsar.Client("pulsar://localhost:6650")  # Connecting to the Pulsar broker
producer = client.create_producer("my-topic")


freq_to_ns = {
    "1Hz": 1_000_000_000,  # 1 second
    "10Hz": 100_000_000,  # 100 milliseconds
    "100Hz": 10_000_000,  # 10 milliseconds
    "1kHz": 1_000_000,  # 1 millisecond
}

selected_freq = "1Hz"
interval_ns = freq_to_ns[selected_freq]


start_ns = time.time_ns()
end_ns = start_ns + (60 * 1_000_000_000)  # Run for 60 seconds


def time_generator(start_ns, end_ns, interval_ns):
    current_ns = start_ns
    while current_ns <= end_ns:
        yield current_ns  # Give the next timestamp
        current_ns += interval_ns  # Move forward in time


for timestamp_ns in time_generator(start_ns, end_ns, interval_ns):
    # 🕒 Create Timestamp
    timestamp = Timestamp()
    timestamp.seconds = timestamp_ns // 1_000_000_000
    timestamp.nanos = timestamp_ns % 1_000_000_000

    # 📊 Create Some Random App Data
    app1 = schema_exec_pb2.AppInfo(
        app_name="App1", exec_time=random.uniform(1, 3), cpu_usage=random.uniform(1, 3)
    )
    app2 = schema_exec_pb2.AppInfo(
        app_name="App2", exec_time=random.uniform(1, 3), cpu_usage=random.uniform(1, 3)
    )

    app_exec_message = schema_exec_pb2.AppExec(
        timestamp=timestamp, num_apps=2, apps=[app1, app2]
    )

    # 📝 Write the MCAP Log to an In-Memory Binary Stream
    with io.BytesIO() as mcap_stream, Writer(mcap_stream) as mcap_writer:
        mcap_writer.write_message(
            topic="topic/app_exec",
            message=app_exec_message,
            log_time=timestamp_ns,
            publish_time=timestamp_ns,
        )
        mcap_data = mcap_stream.getvalue()  # Get the binary data from the stream

    # 📤 Send the Binary Data to Pulsar
    producer.send(mcap_data)  # Send to Pulsar
    print(f"🔥 Published MCAP log at {timestamp.seconds}s, {timestamp.nanos}ns")


client.close()
