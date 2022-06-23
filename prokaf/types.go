package prokaf

import (
	"context"
	"github.com/Shopify/sarama"
	"google.golang.org/protobuf/proto"
)

const (
	version         = "0.1.0"
	headerID        = "prokaf_message_id"
	headerType      = "prokaf_message_type"
	headerAppliesAt = "prokaf_applies_at"
	headerVersion   = "prokaf_version"
)

type Context interface {
	context.Context
	GetMessageType() string
}

type headers map[string][]byte

type MessageDecoder func(in proto.Message) error

type ConsumerMessageHandler func(ctx Context, dec MessageDecoder, ack func(err error)) error

type ProducerMessageHandler func(ctx context.Context, out proto.Message, ack func(error), options ...MessageOption) error

type RegistryEntry struct {
	Handler ConsumerMessageHandler
}

type MessageOption interface {
	apply(headers headers, key sarama.Encoder) (headers, sarama.Encoder)
}

type Client interface {
	StartConsumer(ctx context.Context, groupName string, topics []string, workers int, interceptors ...MessageConsumerInterceptor) error
	StartProducer(ctx context.Context, interceptors ...MessageProducerInterceptor) error
	AddRegistryEntry(name string, handler ConsumerMessageHandler)
	ConsumeMessageProduce(ctx context.Context, out proto.Message, topic string, ack func(error), options ...MessageOption) error
}

type MessageConsumerInterceptor func(ctx Context, dec MessageDecoder, ack func(error), next ConsumerMessageHandler) error

type MessageProducerInterceptor func(ctx context.Context, out proto.Message, ack func(error), next ProducerMessageHandler, options ...MessageOption) error
