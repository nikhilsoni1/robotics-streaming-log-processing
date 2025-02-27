# to-do
- [ ] modify producer to produce both the topics
- [ ] re-do docker file to copy paste /producer_app into docker and run that

# Important Commands
1. docker exec -it pulsar01 bin/pulsar-admin brokers list standalone
2. docker logs pulsar01 --tail 50 -f
3. sudo mkdir -p ./data/zookeeper ./data/bookkeeper
4. sudo chown -R 10000 data
5. sudo chown -R 10000:10000 data
6. sudo chmod -R 755 data
7. sudo chown -R nikhilsoni:staff data
8. docker exec -it broker /bin/sh


# Important Links
1. Run a Pulsar cluster locally with Docker Compose
2. [Getting Started with Docker Compose](https://pulsar.apache.org/docs/4.0.x/getting-started-docker-compose/)

# git tagging
1. git tag -a working-producer-broker -m "Tagging a working copy of producer broker combo"
2. git push origin working-producer-broker