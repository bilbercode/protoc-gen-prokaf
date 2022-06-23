package prokaf

import "sync"

type queueItem struct {
	ack       func(error)
	burn      bool
	completed bool
}

type queue struct {
	items     []*queueItem
	mu        sync.Mutex
	completed bool
}

func (q *queue) Clear() {
	q.mu.Lock()
	q.items = nil
	q.completed = false
	for _, item := range q.items {
		item.burn = true
	}
	q.mu.Unlock()
}

func (q *queue) Enqueue(ack func(error)) func(error) {
	q.mu.Lock()
	item := &queueItem{ack: ack}
	q.items = append(q.items, item)
	q.mu.Unlock()
	return func(err error) {
		q.mu.Lock()
		if item.burn {
			return
		}
		if q.completed {
			q.mu.Unlock()
			return
		}
		if err != nil {
			item.ack(err)
			q.completed = true
			q.mu.Unlock()
			return
		}
		item.completed = true
		var completed []*queueItem
		for _, item := range q.items {
			if !item.completed {
				break
			}
			completed = append(completed, item)
		}
		q.items = q.items[len(completed):]
		q.mu.Unlock()

		for _, item := range completed {
			item.ack(nil)
		}
	}
}
