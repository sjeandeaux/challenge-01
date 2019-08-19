package main

import (
	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"bufio"
	"context"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	awsRegion    = "eu-west-1"
	bucket       = "challenge-technical-test"
	bucketPrefix = "data"
	csv          = ".csv"
	separator    = ","
)

type commandLine struct {
	brokers  string
	topic    string
	ticker   time.Duration
	maxFiles int
}

func (c commandLine) main(ctx context.Context) error {
	ctxUsed := ctx
	if ctx == nil {
		ctxUsed = context.Background()
	}
	brokers := strings.Split(c.brokers, separator)

	ingestator, err := NewIngestator(brokers, c.ticker, c.maxFiles, c.topic)
	if err != nil {
		log.Fatal(err)
	}
	err = ingestator.run(ctxUsed)
	return err

}

var cmdLine = &commandLine{}

func init() {
	flag.StringVar(&cmdLine.brokers, "brokers", "kafka1:9092", "List of brokers (separeted by ,)")
	flag.StringVar(&cmdLine.topic, "topic", "matching-data", "Topic name")
	flag.IntVar(&cmdLine.maxFiles, "maxFiles", -1, "The number of files which are processed (-1 to process all files)")
	flag.DurationVar(&cmdLine.ticker, "ticker", 5*time.Second, "The ticker to fetch file and send it into Kafka")

	flag.Parse()
}

func main() {
	err := cmdLine.main(nil)
	if err != nil {
		log.Fatal(err)
	}
}

// Ingestator it consumes S3 files and produces into kafka
type Ingestator struct {
	svc        s3iface.S3API
	downloader *s3manager.Downloader
	producer   *sarama.SyncProducer
	maxFiles   int
	ticker     time.Duration
	topic      string
}

// NewIngestator create an ingestator
// brokerList kafka brokers
// ticker the ticker to process file and send it into kafka
// maxFiles the number of files managed in process part, -1 all files.
// topic topic name
func NewIngestator(brokerList []string, ticker time.Duration, maxFiles int, topic string) (*Ingestator, error) {

	conf := session.New(&aws.Config{
		Region:                        aws.String(awsRegion),
		Credentials:                   credentials.AnonymousCredentials,
		CredentialsChainVerboseErrors: aws.Bool(false),
		LogLevel:                      aws.LogLevel(aws.LogOff),

		HTTPClient: &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment}, Timeout: 10 * time.Second},
	})

	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	config := sarama.NewConfig()
	config.ClientID = "ingestator" //TODO Use golang to add the version ingestator-v0.0.0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	return &Ingestator{
		svc:        s3.New(conf),
		downloader: s3manager.NewDownloader(conf),
		producer:   &producer,
		maxFiles:   maxFiles,
		topic:      topic,
	}, nil
}

// listObjects get CSV files on S3
// TODO we case use the pagination in case there is too much files
func (ingestator *Ingestator) listObjects() ([]string, error) {
	output, err := ingestator.svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(bucketPrefix),
	})

	if err != nil {
		return nil, err
	}

	objects := []string{}

	for _, p := range output.Contents {
		if strings.HasSuffix(*p.Key, csv) {
			objects = append(objects, *p.Key)
		}
	}
	sort.Strings(objects)
	return objects, nil

}

// run every 5 seconds it sends in kafka raws
func (ingestator *Ingestator) run(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	objects, err := ingestator.listObjects()
	if err != nil {
		return err
	}
	index := 0
	nbObjects := len(objects)
	if nbObjects == 0 {
		return nil
	}
	log.Printf("Nb Objects %d", nbObjects)
	for {
		select {

		case <-ticker.C:
			nbLines := 0
			for nbLines == 0 {
				log.Printf("nbObjects %d index %d ingestator.maxFiles %d", nbObjects, index, ingestator.maxFiles)
				if index >= nbObjects || ingestator.maxFiles == index {
					log.Println("End without error")
					return nil
				}

				nbLines, lines, err := ingestator.readLines(objects[index])
				if err != nil {
					return err
				}
				index++
				log.Printf("Nb Lines %d", nbLines)

				if err := ingestator.produce(lines); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			log.Println("Who killed me?")
			return nil
		}
	}

}

func (ingestator *Ingestator) readLines(indexObject string) (int, []string, error) {
	const PrefixTmpFile = "ingestator-"
	tmpFile, err := ioutil.TempFile(".", PrefixTmpFile)
	if err != nil {
		return 0, nil, err
	}
	defer os.Remove(tmpFile.Name())

	_, err = ingestator.downloader.Download(tmpFile, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(indexObject),
	})
	if err != nil {
		return 0, nil, err
	}

	scanner := bufio.NewScanner(tmpFile)
	lines := []string{}
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return len(lines), lines, nil

}

//produce messages in kafka in topic
func (ingestator *Ingestator) produce(lines []string) error {
	for _, line := range lines {
		log.Println("send in kafka", line)
		partition, offset, err := (*ingestator.producer).SendMessage(&sarama.ProducerMessage{
			Topic: ingestator.topic,
			Value: sarama.StringEncoder(line),
		})
		log.Printf("Partition %d Offset %d", partition, offset)
		if err != nil {
			return err
		}
	}
	return nil

}
