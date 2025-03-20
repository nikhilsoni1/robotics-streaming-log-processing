

# How to Run

1. Start Docker Compose services:
   ```sh
   docker compose up influx_db pulsar01
   ```
   Ensure they are healthy.

2. Create directories for zookeeper and bookeeper to store data
   ```sh
   chmod +x create_dirs.sh
   ./create_dirs.sh
   ```
3. Set up the producer application:
   ```sh
   cd producer_app
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
4. Start the producer:
   ```sh
   python producer.py
   ```
5. Start the consumer:
   ```sh
   python consumer.py
   ```
6. Update the token in the consumer if needed:
   - Access InfluxDB admin panel: [InfluxDB Admin Panel](http://localhost:8086) (username: admin, password: admin123)
   - Access Grafana: [Grafana](http://localhost:3000) (username: admin, password: admin)

# Important Commands
1. docker exec -it pulsar01 bin/pulsar-admin brokers list standalone
2. docker logs pulsar01 --tail 50 -f
3. sudo mkdir -p ./data/zookeeper ./data/bookkeeper
4. sudo chown -R 10000 data
5. sudo chown -R 10000:10000 data
6. sudo chmod -R 755 data
7. sudo chown -R nikhilsoni:staff data
8. docker exec -it broker /bin/sh
9. docker compose logs -f
10. jupyter nbconvert --to script 'delete_influx_buckets.ipynb'
