import time
import files.schema_exec_pb2 as schema_exec_pb2  # Import the compiled Protobuf file
from google.protobuf.timestamp_pb2 import Timestamp
from mcap_protobuf.writer import Writer
import random

# Frequency to nanosecond mapping
freq_to_ns = {
    "1Hz": 1_000_000_000,   # 1 second in nanoseconds
    "10Hz": 100_000_000,    # 100 milliseconds in nanoseconds
    "100Hz": 10_000_000,    # 10 milliseconds in nanoseconds
    "1kHz": 1_000_000       # 1 millisecond in nanoseconds
}

# Set the desired frequency
selected_freq = "10Hz"  # Change this to "1Hz", "10Hz", "100Hz", or "1kHz"
interval_ns = freq_to_ns[selected_freq]

# Define time range using nanoseconds
start_ns = time.time_ns()  # Current time in nanoseconds
end_ns = start_ns + (60 * 1_000_000_000)  # 10,000 seconds later in nanoseconds

# Function to generate timestamps at the selected frequency
def time_generator(start_ns, end_ns, interval_ns):
    current_ns = start_ns
    while current_ns <= end_ns:
        yield current_ns
        current_ns += interval_ns  # Increment by the selected interval

with open("sample-7.mcap", "wb") as f, Writer(f) as mcap_writer:
    for timestamp_ns in time_generator(start_ns, end_ns, interval_ns):
        timestamp = Timestamp()
        timestamp.seconds = timestamp_ns // 1_000_000_000  # Convert to seconds
        timestamp.nanos = timestamp_ns % 1_000_000_000  # Extract remaining nanoseconds

        app1 = schema_exec_pb2.AppInfo(app_name="App1", exec_time=random.uniform(1, 3), cpu_usage=random.uniform(1, 3))
        app2 = schema_exec_pb2.AppInfo(app_name="App2", exec_time=random.uniform(1, 3), cpu_usage=random.uniform(1, 3))
        
        app_exec_message = schema_exec_pb2.AppExec(
            timestamp=timestamp, num_apps=2, apps=[app1, app2]
        )
        
        mcap_writer.write_message(
            topic="topic/app_exec",
            message=app_exec_message,
            log_time=timestamp_ns,
            publish_time=timestamp_ns,
        )
