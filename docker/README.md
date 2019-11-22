# Oceanus Playground

It is based on a [docker-compose](https://docs.docker.com/compose/) environment and super easy to setup.

## Setup

The operations playground requires a custom Docker image in addition to public images for Flink, Kafka, and ZooKeeper. 

The `docker-compose.yaml` file of the operations playground is located in the `docker` directory. Assuming you are at the root directory of the [`oceanus-playgrounds`](https://git.code.oa.com/deryzhou/oceanus-playgrounds) repository, change to the `docker` folder by running

```bash
cd docker/
```

### Building the custom Docker image

Build the Docker image by running

```bash
docker-compose build
```

### Starting the Playground

Once you built the Docker image, run the following command to start the playground

```bash
docker-compose up -d
```

You can check if the playground was successfully started, if you can access the WebUI of the Flink cluster at [http://localhost:8081](http://localhost:8081).

### Stopping the Playground

To stop the playground, run the following command

```bash
docker-compose down
```
