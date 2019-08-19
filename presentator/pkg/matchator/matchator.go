package matchator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	pb "github.com/sjeandeaux/presentator/presentator-grpc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//NewMatchingAggregatorServiceServer instance of server for the matchin aggregator
func NewMatchingAggregatorServiceServer(ctx context.Context, c Config) (*MatchingAggregatorServiceServer, error) {
	client, err := c.getMongoClient()
	if err != nil {
		return nil, err
	}
	database := client.Database(c.Database)

	return &MatchingAggregatorServiceServer{
		client:            client,
		ctx:               ctx,
		driverCollection:  database.Collection(c.DriverCollection),
		requestCollection: database.Collection(c.RequestCollection),
	}, nil
}

//Config to connect to database
type Config struct {
	Host     string
	Port     int
	Login    string
	Password string

	Database          string
	DriverCollection  string
	RequestCollection string
}

func (c *Config) getMongoURI() string {
	return fmt.Sprintf("mongodb://%s:%s@%s:%d/?authSource=admin", c.Login, c.Password, c.Host, c.Port)
}

func (c *Config) getMongoClient() (*mongo.Client, error) {
	return mongo.Connect(context.Background(), options.Client().ApplyURI(c.getMongoURI()))
}

// MatchingAggregatorServiceServer matching on mongo
type MatchingAggregatorServiceServer struct {
	client *mongo.Client

	driverCollection  *mongo.Collection
	requestCollection *mongo.Collection

	ctx context.Context
}

// Close close the connection
func (m *MatchingAggregatorServiceServer) Close() {
	m.client.Disconnect(m.ctx)
}

//MatchingAggregatorRequest the aggregation
func (m *MatchingAggregatorServiceServer) MatchingAggregatorRequest(ctx context.Context, r *pb.Request) (*pb.Reply, error) {
	return m.matchingAggregator(ctx, r, m.requestCollection)
}

//MatchingAggregatorDriver the aggregation
func (m *MatchingAggregatorServiceServer) MatchingAggregatorDriver(ctx context.Context, r *pb.Request) (*pb.Reply, error) {
	return m.matchingAggregator(ctx, r, m.driverCollection)
}

func (m *MatchingAggregatorServiceServer) matchingAggregator(ctx context.Context, r *pb.Request, collection *mongo.Collection) (*pb.Reply, error) {
	if r == nil {
		return nil, errors.New("Houston, you should file the request")
	}
	point := r.GetPoint()
	if r.GetPoint() == nil {
		return nil, errors.New("Houston, you should file the point in your request")
	}
	log.Printf("Request %v", r)

	ctx, _ = context.WithTimeout(context.Background(), 30*time.Second)

	findOptions := options.Find()
	findOptions.SetLimit(r.Precision)
	findOptions.SetSort(bson.M{"computeAt": -1})
	rBSON := bson.M{
		"polygon": bson.M{
			"$geoIntersects": bson.M{
				"$geometry": bson.M{
					"type":        "Point",
					"coordinates": []float64{point.GetLongitude(), point.GetLatitude()},
				},
			},
		},
		"computeAt": bson.M{
			"$lte": r.GetComputeAt(),
		},
	}

	log.Println("Request BSON", rBSON)

	cur, err := collection.Find(ctx, rBSON, findOptions)

	if err != nil {
		return nil, err
	}

	defer cur.Close(ctx)

	type Result struct {
		Rate      float64 `bson:"rate"`
		ComputeAt int64   `bson:"computeAt"`
	}

	index := 0.0
	reply := &pb.Reply{Result: 0.0}
	for cur.Next(ctx) {
		var result Result
		err := cur.Decode(&result)
		if err != nil {
			return nil, err
		}
		log.Println(result)
		reply.Result += r.GetSmoothing() * math.Pow(1-r.GetSmoothing(), index) * result.Rate
		index++
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}

	return reply, nil
}
