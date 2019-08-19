# Ingestator

> `ingestator` ingests data from S3 into kafka.

## Requirements

### tools

- make
- go
- docker
- docker-compose

## How do we play with it

You should install some go tools:

```bash
make download
make help
```

### Run the unit test

This makefile goal calls `gocov`.
```
make test-coverage
```

### Run the integration test

> The integration test run inside docker-compose.

> The docker-compose builds a simple infrastructure with one zookeeper and one kafka.

> It tests that the consumed messages from S3 are in kafka.

```
make docker-compose-it-test
```

### Run the application

```sh
ingestator -h
Usage of ingestator:
  -brokers string
        List of brokers (separeted by ,) (default "kafka1:9092")
  -maxFiles int
        The number of files which are processed (-1 to process all files) (default -1)
  -ticker duration
        The ticker to fetch file and send it into Kafka (default 5s)
  -topic string
        Topic name (default "matching-data")
```
