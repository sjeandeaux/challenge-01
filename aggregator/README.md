# Aggregator

> It is a spark streaming job that exploits the ingested data (by the [ingestator](../ingestator)) in order to aggregate driver match rates and request match rates by geo-spatial units (squares) and a configuration time period (spark batch window).
> It stores the output results in mongodb.

## Requirements

- make
- gradle
- java
- scala

## Integration test

To run the integration test:
* `make docker-compose-up`: spawns the containers
* `make docker-compose-it-test`: runs the integration tests.