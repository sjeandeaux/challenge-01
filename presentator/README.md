# presentator

> it reads the data from mongodb. It exposes an endpoint which takes as parameters a position (latitude and longitude), a timestamp, a smoothing parameter α ∈ [0;1] and a precision parameter p and that will return the adjusted values of request and driver match rate.


## Requirements

### tools

- make
- go
- protoc
- docker
- docker-compose

## How do we play with it

You should install some go tools:

```bash
make download
make help
```

### Run the integration test

> The integration test run inside docker-compose.

> The docker-compose builds a simple infrastructure with one mongodb.

> It tests the results of HTTP calls.

```
docker-compose-up
make docker-compose-it-test
make docker-compose-postman
```

### Run the application

Generate the binary: `go build .`
Run the binary: 

```sh
presentator -help
Usage of presentator:
  -grpc-port int
        The grpc port (default 8080)
  -http-port int
        The http port (default 8686)
  -mongo-database string
        The database (default "challenge")
  -mongo-driver-collection string
        The driver collection (default "driverRate")
  -mongo-host string
        The mongo host (default "mongo")
  -mongo-login string
        The mongo login (default "devroot")
  -mongo-password string
        The mongo password (it should a secret but out of laziness... (default "devroot")
  -mongo-port int
        The mongo port (default 27017)
  -mongo-request-collection string
        The request collection (default "requestRate")
```
