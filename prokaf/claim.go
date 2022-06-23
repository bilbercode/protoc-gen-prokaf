package prokaf

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"hash/fnv"
)

type claim struct {
	registry     map[string]ConsumerMessageHandler
	hostname     string
	interceptors []MessageConsumerInterceptor
	q            *queue
	workerCount  int
	workers      []chan *sarama.ConsumerMessage
	workerCtx    context.Context
	workerCancel context.CancelFunc
	workerGroup  *errgroup.Group
	workerErr    chan error
}

func (c *claim) Setup(session sarama.ConsumerGroupSession) error {
	// if we have a positive worker count create n workers to process
	// messages, this will fan-in. worker counts of 0 or less will
	// result in the consumer creating a consumer handler per accepted claim.
	if c.workerCount > 0 {
		c.workerErr = make(chan error)
		ctx := session.Context()
		ctx, cancel := context.WithCancel(ctx)
		c.workerGroup, c.workerCtx = errgroup.WithContext(ctx)
		c.workerCancel = cancel

		for i := 0; i < c.workerCount; i++ {
			// create a chanel unique to this worker, this is so that
			// we can route messages based on message key index to the same
			// routine to ensure message ordering on the consumer
			ch := make(chan *sarama.ConsumerMessage)
			c.workers = append(c.workers, ch)
			c.workerGroup.Go(func() error {
				for {
					select {
					case <-c.workerCtx.Done():
						return nil
					case msg, ok := <-ch:
						switch {
						case !ok:
							return nil
						case msg == nil:
							continue
						default:
							ctx := newCtx(ctx, msg.Headers)
							handler, ok := c.registry[ctx.GetMessageType()]
							if !ok {
								// ignore this message as we don't have a handler registered
								// we simply enqueue and ack, this will ensure were still acking in the
								// correct sequence
								c.q.Enqueue(func(err error) {
									session.MarkMessage(msg, c.hostname)
								})(nil)
								continue
							}
							ack := c.q.Enqueue(func(err error) {
								session.MarkMessage(msg, c.hostname)
							})
							unmarshal := func(in proto.Message) error {
								return proto.Unmarshal(msg.Value, in)
							}
							// send the message though the interceptors to the final handler
							err := c.next(ctx, 0, unmarshal, ack, handler)
							if err != nil {
								// We have an error notify the workers and a main loop
								select {
								case <-ctx.Done():
								case c.workerErr <- err:
								}
								return err
							}
						}
					}
				}
			})
		}
	}
	return nil
}

func (c *claim) Cleanup(_ sarama.ConsumerGroupSession) error {
	if c.workerCount > 0 {
		// make the workers bail and close their channels
		c.workerCancel()
		err := c.workerGroup.Wait()
		for _, ch := range c.workers {
			close(ch)
		}
		close(c.workerErr)
		if err != nil {
			return err
		}
	}
	c.q.Clear()
	return nil
}

func (c *claim) ConsumeClaim(session sarama.ConsumerGroupSession, groupClaim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()
	group, ctx := errgroup.WithContext(ctx)
	if c.workerCount > 0 {
		group.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case err, ok := <-c.workerErr:
					switch {
					case !ok:
						return nil
					case err == nil:
						continue
					default:
						return err
					}
				}
			}
		})
	}
	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case msg, ok := <-groupClaim.Messages():
				switch {
				case !ok:
					return nil
				case msg == nil:
					// Skip trash messages
					continue
				default:
					if c.workerCount < 0 {
						ctx := newCtx(ctx, msg.Headers)
						handler, ok := c.registry[ctx.GetMessageType()]
						if !ok {
							c.q.Enqueue(func(err error) {
								session.MarkMessage(msg, c.hostname)
							})(nil)
							continue
						}
						ack := c.q.Enqueue(func(err error) {
							session.MarkMessage(msg, c.hostname)
						})

						err := c.next(ctx, 0, func(in proto.Message) error {
							return proto.Unmarshal(msg.Value, in)
						}, ack, handler)
						if err != nil {
							return err
						}
						continue
					}
					// send the message to a channel based on the key to keep message ordering intact
					hash := fnv.New64()
					_, _ = hash.Write(msg.Key)
					worker := c.workers[hash.Sum64()%uint64(c.workerCount)]
					select {
					case <-ctx.Done():
					case worker <- msg:
					}
				}
			}
		}
	})

	return group.Wait()
}

func (c *claim) next(ctx Context, i int, dec MessageDecoder, ack func(error), fn ConsumerMessageHandler) error {
	switch {
	case i > len(c.interceptors):
		return errors.New("attempt to access out of bounds consumer interceptor")
	case i == len(c.interceptors):
		return fn(ctx, dec, ack)
	default:
		err := c.interceptors[i](ctx, dec, ack, fn)
		if err != nil {
			return err
		}
		return c.next(ctx, i+1, dec, ack, fn)
	}
}
