import time
import files.schema_exec_pb2 as schema_exec_pb2  # Import the compiled Protobuf file
from google.protobuf.timestamp_pb2 import Timestamp
from mcap_protobuf.writer import Writer

# Create an instance of AppExec message
timestamp = Timestamp()
timestamp.GetCurrentTime()  # Get current timestamp in protobuf format

with open("sample.mcap", "wb") as f, Writer(f) as mcap_writer:
    for i in range(1, 11):
        app1 = schema_exec_pb2.AppInfo(app_name="App1", exec_time=1.23, cpu_usage=12.5)
        app2 = schema_exec_pb2.AppInfo(app_name="App2", exec_time=2.34, cpu_usage=25.0)
        app_exec_message = schema_exec_pb2.AppExec(
            timestamp=timestamp, num_apps=2, apps=[app1, app2]
        )
        mcap_writer.write_message(
            topic="topic/app_exec",
            message=app_exec_message,
            log_time=i * 1000,
            publish_time=i * 1000,
        )
