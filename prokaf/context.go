package prokaf

import (
	"context"
	"github.com/Shopify/sarama"
	"time"
)

type messageContext struct {
	context.Context
	headers []*sarama.RecordHeader
}

func newCtx(ctx context.Context, headers []*sarama.RecordHeader) Context {
	return &messageContext{
		Context: ctx,
		headers: headers,
	}
}

func (c *messageContext) GetMessageType() string {
	for _, h := range c.headers {
		if string(h.Key) == headerType {
			return string(h.Value)
		}
	}
	return ""
}

func (c *messageContext) GetAppliesAt() (time.Time, error) {
	appliesAt := time.Now()
	for _, h := range c.headers {
		if string(h.Key) == headerAppliesAt {
			err := appliesAt.UnmarshalText(h.Value)
			if err != nil {
				panic(err.Error())
			}
		}
	}
	return appliesAt, nil
}

func (c *messageContext) GetID() string {
	for _, h := range c.headers {
		if string(h.Key) == headerID {
			return string(h.Value)
		}
	}
	return ""
}

func (c *messageContext) GetVersion() string {
	for _, h := range c.headers {
		if string(h.Key) == headerVersion {
			return string(h.Value)
		}
	}
	return ""
}
