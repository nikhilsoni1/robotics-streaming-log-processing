import json
import sys
import os
import shutil
from time import time_ns, sleep
from mcap.writer import Writer
from random import uniform, choice

# Define directory for log storage
LOG_DIR = "./mcap_logs"

# Remove and recreate the directory if it exists
if os.path.exists(LOG_DIR):
    shutil.rmtree(LOG_DIR)
os.makedirs(LOG_DIR)

# Define schema versions and their fields
SCHEMA_VERSIONS = {
    "CriticalSystemInfo": [
        "timestamp_ns", "app_execution_time_ms", "cpu_usage", "memory_usage_mb",
        "sensor_health_status", "network_usage_kbps", "temperature"
    ],
    "CriticalSystemInfoV2": [
        "timestamp_ns", "app_execution_time_ms", "cpu_usage", "memory_usage_mb",
        "sensor_health_status", "network_usage_kbps", "battery_voltage_v", "temperature"
    ],
    "CriticalSystemInfoV3": [
        "timestamp_ns", "cpu_usage", "memory_usage_mb", "sensor_health_status", 
        "network_usage_kbps", "battery_voltage_v", "disk_io_mbps", "power_consumption_w", "temperature"
    ]
}

# Define sample data generation function
def generate_data(schema_name):
    data = {
        "timestamp_ns": time_ns(),
        "app_execution_time_ms": uniform(5, 20),
        "cpu_usage": uniform(10, 90),
        "memory_usage_mb": uniform(500, 2000),
        "sensor_health_status": choice(["OK", "WARNING", "ERROR"]),
        "network_usage_kbps": uniform(100, 1000),
        "battery_voltage_v": uniform(20, 30),
        "disk_io_mbps": uniform(50, 1000),
        "power_consumption_w": uniform(100, 400),
        "temperature": [uniform(30, 80) for _ in range(3)]
    }
    return {key: data[key] for key in SCHEMA_VERSIONS[schema_name]}

# MCAP Writer Setup
def write_mcap_log(schema_name, duration=10):
    filename = os.path.join(LOG_DIR, f"{schema_name}.mcap")
    with open(filename, "wb") as stream:
        writer = Writer(stream)
        writer.start()
        schema_id = writer.register_schema(
            name=schema_name,
            encoding="jsonschema",
            data=json.dumps({"type": "object", "properties": {key: {"type": "number" if "temperature" not in key else "array"} for key in SCHEMA_VERSIONS[schema_name]}}).encode(),
        )
        channel_id = writer.register_channel(
            schema_id=schema_id,
            topic=schema_name,
            message_encoding="json",
        )
        start_time = time_ns()
        while (time_ns() - start_time) / 1e9 < duration:
            data = generate_data(schema_name)
            writer.add_message(
                channel_id=channel_id,
                log_time=time_ns(),
                data=json.dumps(data).encode("utf-8"),
                publish_time=time_ns(),
            )
            sleep(1)  # Adjust frequency as needed
        writer.finish()
    print(f"{schema_name} logs written to {filename}")

# Run the log generation for all schemas
def main():
    print("Starting MCAP log generation...")
    for schema in SCHEMA_VERSIONS.keys():
        write_mcap_log(schema)
    print("Log generation complete.")

if __name__ == "__main__":
    main()
