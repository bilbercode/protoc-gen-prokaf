package test

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/bilbercode/protoc-gen-prokaf/prokaf"
	"github.com/bilbercode/protoc-gen-prokaf/test/pkg/example"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

var (
	client prokaf.Client
	svc    *service
)

type service struct {
	example.UnimplementedConsumer
	messages []proto.Message
	received chan interface{}
}

func (s *service) ConsumeTestOne(ctx prokaf.Context, in *example.TestOne, ack func(err error)) error {
	s.messages = append(s.messages, in)
	ack(nil)
	s.received <- nil
	return nil
}

func (s *service) ConsumeTestTwo(ctx prokaf.Context, in *example.TestTwo, ack func(err error)) error {
	s.messages = append(s.messages, in)
	ack(nil)
	s.received <- nil
	return nil
}

func TestMain(m *testing.M) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 3
	config.Producer.Timeout = time.Minute
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Version = sarama.MaxVersion
	c, err := sarama.NewClient([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err.Error())
	}
	client = prokaf.NewProKafClient(c, "localhost")
	svc = &service{
		received: make(chan interface{}),
	}
	example.RegisterProKafConsumerHandlersFromService(svc, client)
	m.Run()
}

func TestWithMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	group, ctx := errgroup.WithContext(ctx)
	producer := example.NewProKafProducer(client)

	group.Go(func() error {
		return client.StartConsumer(ctx, "test-group", []string{"test"}, -1)
	})

	group.Go(func() error {
		return client.StartProducer(ctx)
	})

	group.Go(func() error {
		return producer.ProduceTestTwo(ctx, &example.TestTwo{
			Id:       "one",
			Username: "two",
		}, "test", func(err error) {
			// TODO (bilbercode) handle this correctly
			if err != nil {
				panic(err.Error())
			}
		}, prokaf.WithID("foo"))
	})

	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-svc.received:
				cancel()
				return nil
			}
		}
	})

	err := group.Wait()
	assert.EqualError(t, err, context.Canceled.Error())

}
