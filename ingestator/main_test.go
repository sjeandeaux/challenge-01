package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/assert"
)

func TestITMain(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short")
	}

	time.Sleep(10 * time.Second)

	const (
		broker = "kafka1:9092"
		topic  = "ingestator-it-test-topic"
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	c := commandLine{brokers: broker, ticker: 1 * time.Second, topic: topic, maxFiles: -1}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.main(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	message := consume(t, broker, topic)
	assert.NotEmpty(t, message, "Houston, we do not have your message")
	cancel()
	wg.Wait()
}

func consume(t *testing.T, broker string, topic string) string {
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_11_0_2
	conf.ClientID = "ingestator-it-test"
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumer([]string{broker}, conf)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	boom := time.NewTicker(60 * time.Second)
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			return string(msg.Value)
		case <-boom.C:
			return ""
		}
	}

}

func TestIngestator_listObjects(t *testing.T) {
	type fields struct {
		svc        s3iface.S3API
		downloader *s3manager.Downloader
		producer   *sarama.SyncProducer
		maxFiles   int
		ticker     time.Duration
		topic      string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []string
		wantErr bool
	}{
		{
			name: "ListObject is on failure",
			fields: fields{
				svc: &mockS3{err: errors.New("booom")},
			},
			wantErr: true,
		},

		{
			name: "ListObject is on OK",
			fields: fields{
				svc: &mockS3{
					output: &s3.ListObjectsV2Output{
						Contents: []*s3.Object{
							{
								Key: aws.String("no.pdf"),
							},
							{
								Key: aws.String("file_2.csv"),
							},
							{
								Key: aws.String("file.csv"),
							},
							{
								Key: aws.String("no.txt"),
							},
						},
					},
				},
			},
			wantErr: false,
			want:    []string{"file.csv", "file_2.csv"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ingestator := &Ingestator{
				svc:        tt.fields.svc,
				downloader: tt.fields.downloader,
				producer:   tt.fields.producer,
				maxFiles:   tt.fields.maxFiles,
				ticker:     tt.fields.ticker,
				topic:      tt.fields.topic,
			}
			got, err := ingestator.listObjects()

			if (err != nil) != tt.wantErr {
				t.Errorf("Ingestator.listObjects() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.EqualValues(t, got, tt.want)

		})
	}
}

type mockS3 struct {
	s3iface.S3API
	err    error
	output *s3.ListObjectsV2Output
}

func (m *mockS3) ListObjectsV2(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return m.output, m.err
}
