import time
import pulsar
import files.schema_exec_pb2 as schema_exec_pb2
import files.schema_sensor_health_pb2 as schema_sensor_health_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from mcap_protobuf.writer import Writer
import random
import io
import time


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
last_time = time.monotonic_ns()
while True:

    # Create Timestamp in Protobuf Format
    timestamp_ns = time.time_ns()
    timestamp = Timestamp()
    timestamp.seconds = timestamp_ns // 1_000_000_000
    timestamp.nanos = timestamp_ns % 1_000_000_000

    # Generate Synthetic Data ----------------------------------------------
    exec_time = random.uniform(1, 3)
    
    # CPU usage increases proportionally to exec_time
    cpu_usage = 1.5 + (exec_time * 0.8) + random.uniform(-0.2, 0.2)  # Adding some variance
    
    # Sensor Temperature increases as CPU usage increases
    sensor_base_temp = 20 + (cpu_usage * 3)  # Base relation
    sensor_temps = [sensor_base_temp + random.uniform(-2, 2) for _ in range(3)]  # Adding some noise

    # Introduce Anomalies
    if random.random() < 0.05:  # 5% chance for a sudden spike
        exec_time *= random.uniform(2, 5)  # Large spike in exec_time
        cpu_usage *= random.uniform(1.5, 3)  # CPU spike follows
        sensor_temps = [random.uniform(80, 100) for _ in range(3)]  # Overheating event

    # Gradual Drift Anomaly (Exec time and CPU increase over long periods)
    drift_factor = 0.0005 * (timestamp.seconds % 1000)
    exec_time += drift_factor
    cpu_usage += drift_factor * 0.8
    sensor_temps = [temp + drift_factor * 2 for temp in sensor_temps]  # Temperature drift
    # Generate Synthetic Data ----------------------------------------------

    # Create Synthetic Data for the AppExec Message
    app1 = schema_exec_pb2.AppInfo(
        app_name="App1", exec_time=exec_time, cpu_usage=cpu_usage
    )
    app2 = schema_exec_pb2.AppInfo(
        app_name="App2", exec_time=exec_time*0.9, cpu_usage=cpu_usage*0.9
    )

    app_exec_message = schema_exec_pb2.AppExec(
        timestamp=timestamp, num_apps=2, apps=[app1, app2]
    )

    # Create Synthetic Data for the SensorHealth Message
    sensor_health_message = schema_sensor_health_pb2.SensorHealth(
        timestamp=timestamp,
        num_sensors=3,
        sensor_temps=sensor_temps,
    )

    # Write the MCAP Log to an In-Memory Binary Stream
    mcap_stream = io.BytesIO()
    mcap_writer = Writer(mcap_stream)
    _log_ts = time.time_ns()

    # make this 100hz
    mcap_writer.write_message(
        topic="topic/app_exec",
        message=app_exec_message,
        log_time= _log_ts,
        publish_time=_log_ts,
    )

    # make this 50hz
    interval_ns = 0.02 * 1_000_000_000
    if time.monotonic_ns() - last_time > interval_ns:
        mcap_writer.write_message(
            topic="topic/sensor_health",
            message=sensor_health_message,
            log_time=_log_ts,
            publish_time=_log_ts,
        )
        last_time = time.monotonic_ns()
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
