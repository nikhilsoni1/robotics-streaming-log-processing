import time
import pulsar
import files.schema_exec_pb2 as schema_exec_pb2  # Import the Protobuf schema
from google.protobuf.timestamp_pb2 import Timestamp
from mcap_protobuf.writer import Writer
import random

# 🎯 Step 1: Connect to Pulsar
client = pulsar.Client('pulsar://localhost:6650')  # Connecting to the Pulsar broker
producer = client.create_producer('mcap-topic')  # Creating a producer for our topic

# ⏳ Step 2: Frequency Config (1Hz to 1kHz options)
freq_to_ns = {
    "1Hz": 1_000_000_000,   # 1 second
    "10Hz": 100_000_000,    # 100 milliseconds
    "100Hz": 10_000_000,    # 10 milliseconds
    "1kHz": 1_000_000       # 1 millisecond
}

selected_freq = "10Hz"  
interval_ns = freq_to_ns[selected_freq]

# ⏳ Step 3: Set the Time Range
start_ns = time.time_ns()
end_ns = start_ns + (60 * 1_000_000_000)  # Run for 60 seconds

# 💡 Step 4: Function to Generate Timestamps at Selected Frequency
def time_generator(start_ns, end_ns, interval_ns):
    current_ns = start_ns
    while current_ns <= end_ns:
        yield current_ns  # Give the next timestamp
        current_ns += interval_ns  # Move forward in time

# 🚀 Step 5: Start Producing Messages
for timestamp_ns in time_generator(start_ns, end_ns, interval_ns):
    # 🕒 Create Timestamp
    timestamp = Timestamp()
    timestamp.seconds = timestamp_ns // 1_000_000_000  
    timestamp.nanos = timestamp_ns % 1_000_000_000  

    # 📊 Create Some Random App Data
    app1 = schema_exec_pb2.AppInfo(app_name="App1", exec_time=random.uniform(1, 3), cpu_usage=random.uniform(1, 3))
    app2 = schema_exec_pb2.AppInfo(app_name="App2", exec_time=random.uniform(1, 3), cpu_usage=random.uniform(1, 3))

    app_exec_message = schema_exec_pb2.AppExec(
        timestamp=timestamp, num_apps=2, apps=[app1, app2]
    )

    # 📝 Write the MCAP Log to a Temporary File
    with open("temp.mcap", "wb") as f, Writer(f) as mcap_writer:
        mcap_writer.write_message(
            topic="topic/app_exec",
            message=app_exec_message,
            log_time=timestamp_ns,
            publish_time=timestamp_ns,
        )

    # 📤 Read the MCAP File & Send It to Pulsar
    with open("temp.mcap", "rb") as f:
        mcap_data = f.read()

    producer.send(mcap_data)  # Send to Pulsar
    print(f"🔥 Published MCAP log at {timestamp.seconds}s, {timestamp.nanos}ns")

# 🛑 Step 6: Close the Connection When Done
client.close()
