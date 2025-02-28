# How to Run

1. Start Docker Compose services:
   ```sh
   docker compose up influx_db pulsar01
   ```
   Ensure they are healthy.
2. Set up the producer application:
   ```sh
   cd producer_app
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. Start the producer:
   ```sh
   python producer.py
   ```
4. Start the consumer:
   ```sh
   python consumer.py
   ```
5. Update the token in the consumer if needed:
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

# Important Links
1. [Run a Pulsar cluster locally with Docker Compose](https://pulsar.apache.org/docs/4.0.x/getting-started-docker-compose/)

# Git Tagging
1. git tag -a v1.0 -m "working copy for casestudy"
2. git push origin v1.0
3. git tag -a working-copy -m "Tagging the latest working copy"
4. git push origin working-copy

# Protobuf Commands
1. protoc --proto_path=files/schema --python_out=files/ files/schema/*.proto

# To-Do
- [ ] Modify producer to produce both the topics
- [ ] Re-do Dockerfile to copy /producer_app into Docker and run that