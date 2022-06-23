package prokaf

import (
	"github.com/Shopify/sarama"
	"time"
)

type option struct {
	fn func(headers headers, key sarama.Encoder) (headers, sarama.Encoder)
}

func (o *option) apply(headers headers, key sarama.Encoder) (headers, sarama.Encoder) {
	return o.fn(headers, key)
}

func newMessageOptionFunc(fn func(headers headers, key sarama.Encoder) (headers, sarama.Encoder)) MessageOption {
	return &option{fn: fn}
}

func WithKey(key []byte) MessageOption {
	return newMessageOptionFunc(func(h headers, k sarama.Encoder) (headers, sarama.Encoder) {
		return h, sarama.ByteEncoder(key)
	})
}

func WithID(id string) MessageOption {
	return newMessageOptionFunc(func(headers headers, key sarama.Encoder) (headers, sarama.Encoder) {
		headers[headerID] = []byte(id)
		return headers, key
	})
}

func WithAppliesAt(t time.Time) MessageOption {
	return newMessageOptionFunc(func(headers headers, key sarama.Encoder) (headers, sarama.Encoder) {
		headers[headerAppliesAt] = []byte(t.Format(time.RFC3339Nano))
		return headers, key
	})
}
