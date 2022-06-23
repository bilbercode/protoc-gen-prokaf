package prokaf

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"hash/fnv"
	"sync"
	"time"
)

type outboundMessage struct {
	msg          proto.Message
	topic        string
	options      []MessageOption
	interceptors []MessageProducerInterceptor
	ack          func(error)
}

type pk struct {
	mu       sync.Mutex
	client   sarama.Client
	hostname string
	registry map[string]ConsumerMessageHandler
	outbound chan *outboundMessage
}

func NewProKafClient(client sarama.Client, hostname string) Client {
	return &pk{
		client:   client,
		hostname: hostname,
		registry: make(map[string]ConsumerMessageHandler),
		outbound: make(chan *outboundMessage),
	}
}

func (p *pk) AddRegistryEntry(name string, handler ConsumerMessageHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.registry[name] = handler
}

func (p *pk) StartProducer(ctx context.Context, interceptors ...MessageProducerInterceptor) error {
	prod, err := sarama.NewAsyncProducerFromClient(p.client)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case success, ok := <-prod.Successes():
				switch {
				case !ok:
					return nil
				case success == nil:
					continue
				default:
					success.Metadata.(func(error))(nil)
				}
			}
		}
	})

	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case err, ok := <-prod.Errors():
				switch {
				case !ok:
					return nil
				case err == nil:
					continue
				default:
					err.Msg.Metadata.(func(error))(err.Err)
				}
			}
		}
	})

	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case msg, ok := <-p.outbound:
				switch {
				case !ok:
					return nil
				case msg == nil:
					continue
				default:
					b, err := proto.Marshal(msg.msg)
					if err != nil {
						return fmt.Errorf("failed to marshal outbound message: %w", err)
					}

					sMsg := sarama.ProducerMessage{
						Topic:    msg.topic,
						Value:    sarama.ByteEncoder(b),
						Metadata: msg.ack,
					}
					headerMap := headers{
						headerType:      []byte(msg.msg.ProtoReflect().Type().Descriptor().FullName()),
						headerID:        []byte(uuid.New().String()),
						headerAppliesAt: []byte(time.Now().Format(time.RFC3339Nano)),
						headerVersion:   []byte(version),
					}
					var key sarama.Encoder
					for _, opt := range msg.options {
						headerMap, key = opt.apply(headerMap, key)
					}

					for k, v := range headerMap {
						sMsg.Headers = append(sMsg.Headers, sarama.RecordHeader{Key: []byte(k), Value: v})
					}

					if key == nil {
						hash := fnv.New64()
						_, _ = hash.Write(b)
						sMsg.Key = sarama.StringEncoder(hex.EncodeToString(hash.Sum(nil)))
					} else {
						sMsg.Key = key
					}

					err = p.next(ctx, 0, msg.msg, msg.ack,
						func(ctx context.Context, out proto.Message, ack func(error), options ...MessageOption) error {
							select {
							case <-ctx.Done():
							case prod.Input() <- &sMsg:
							}
							return nil
						}, interceptors, msg.options...)

				}
			}
		}
	})

	return group.Wait()
}

func (p *pk) next(ctx context.Context, i int, out proto.Message, ack func(error),
	next ProducerMessageHandler, interceptors []MessageProducerInterceptor, options ...MessageOption) error {
	switch {
	case i > len(interceptors):
		return errors.New("attempt to access out of bounds producer interceptor")
	case i == len(interceptors):
		return next(ctx, out, ack, options...)
	default:
		err := interceptors[i](ctx, out, ack, func(ctx context.Context, out proto.Message, ack func(error), options ...MessageOption) error {
			return p.next(ctx, i+1, out, ack, next, interceptors, options...)
		}, options...)
		if err != nil {
			return err
		}
		return nil
	}
}

func (p *pk) StartConsumer(ctx context.Context, groupName string, topics []string, workers int, interceptors ...MessageConsumerInterceptor) error {
	c, err := sarama.NewConsumerGroupFromClient(groupName, p.client)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	for {
		err = c.Consume(
			ctx,
			topics,
			&claim{
				registry:     p.registry,
				hostname:     p.hostname,
				interceptors: interceptors,
				workerCount:  workers,
				q:            &queue{},
			})
		switch {
		case err == context.Canceled:
			continue
		case err != nil:
			return err
		case ctx.Err() != nil:
			return ctx.Err()
		}
	}
}

func (p *pk) ConsumeMessageProduce(ctx context.Context, out proto.Message, topic string, ack func(error), options ...MessageOption) error {
	select {
	case <-ctx.Done():
		return nil
	case p.outbound <- &outboundMessage{
		msg:     out,
		topic:   topic,
		options: options,
		ack:     ack,
	}:
		return nil
	}
}
