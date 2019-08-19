package matchator

import (
	"context"
	"testing"

	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/stretchr/testify/assert"

	pb "github.com/sjeandeaux/presentator/presentator-grpc"

	"go.mongodb.org/mongo-driver/x/bsonx"
)

func TestConfig_getMongoURI(t *testing.T) {
	type fields struct {
		Host              string
		Port              int
		Login             string
		Password          string
		DriverCollection  string
		RequestCollection string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "generate URI",
			fields: fields{
				Host:     "mongo",
				Login:    "mongo",
				Password: "mongo",
				Port:     666,
			},
			want: "mongodb://mongo:mongo@mongo:666/?authSource=admin",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				Host:              tt.fields.Host,
				Port:              tt.fields.Port,
				Login:             tt.fields.Login,
				Password:          tt.fields.Password,
				DriverCollection:  tt.fields.DriverCollection,
				RequestCollection: tt.fields.RequestCollection,
			}
			if got := c.getMongoURI(); got != tt.want {
				t.Errorf("Config.getMongoURI() = %v, want %v", got, tt.want)
			}
		})
	}
}

var matchingAggregatorDriverData = []interface{}{
	createRate(1506880805, 0.5),
	createRate(1506880804, 0.0),
	createRate(1506880803, 0.0),
}

var matchingAggregatorRequestData = []interface{}{
	createRate(1506880805, 0.1),
	createRate(1506880804, 0.5),
	createRate(1506880803, 0.8),
}

func TestMatchingAggregatorServiceServer_MatchingAggregatorCollectionOK(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short")
	}
	const (
		mongoURI          = "mongodb://devroot:devroot@localhost:27017/?authSource=admin"
		databaseName      = "challenge"
		driverCollection  = "driverRate"
		requestCollection = "requestRate"
	)
	//init
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Minute)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))

	assert.NoError(t, err)

	database := client.Database(databaseName)

	m := &MatchingAggregatorServiceServer{
		client:            client,
		ctx:               ctx,
		driverCollection:  database.Collection("driverRate"),
		requestCollection: database.Collection("requestRate"),
	}

	//build the fixtures
	data := []struct {
		rates      []interface{}
		expected   *pb.Reply
		collection *mongo.Collection
		request    *pb.Request
	}{
		{
			rates:      matchingAggregatorDriverData,
			request:    &pb.Request{Point: &pb.Point{Longitude: 48.915516, Latitude: 2.241223}, ComputeAt: 1506880805, Precision: 3, Smoothing: 0.1},
			expected:   &pb.Reply{Result: 0.05},
			collection: m.driverCollection,
		},
		{
			rates:      matchingAggregatorRequestData,
			request:    &pb.Request{Point: &pb.Point{Longitude: 48.915516, Latitude: 2.241223}, ComputeAt: 1506880805, Precision: 3, Smoothing: 0.1},
			expected:   &pb.Reply{Result: 0.11980000000000002},
			collection: m.requestCollection,
		},
	}

	//run the test
	for _, test := range data {
		err = test.collection.Drop(ctx)
		assert.NoError(t, err)

		_, err = test.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bsonx.Doc{{"location", bsonx.String("2d")}},
		})
		assert.NoError(t, err)

		_, err = test.collection.InsertMany(ctx, test.rates)
		assert.NoError(t, err)

		r, err := m.matchingAggregator(ctx, test.request, test.collection)
		assert.NoError(t, err)
		assert.Equal(t, r, test.expected)

	}

}

type Rate struct {
	PolygonName string
	ComputeAt   int64 `bson:"computeAt"`
	Rate        float64
	Polygon     Polygon
}

type Polygon struct {
	TypeOf      string `bson:"type"`
	Coordinates [][][]float64
}

func createRate(compateAt int64, rate float64) interface{} {
	return &Rate{
		PolygonName: "NW",
		ComputeAt:   compateAt,
		Rate:        rate,
		Polygon: Polygon{
			TypeOf: "Polygon",
			Coordinates: [][][]float64{[][]float64{
				[]float64{48.961241, 2.113474},
				[]float64{49.109925, 2.335517},
				[]float64{48.882609, 2.336188},
				[]float64{48.844305, 2.033190},
				[]float64{48.961241, 2.113474}}},
		},
	}
}
