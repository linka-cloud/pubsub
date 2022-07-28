package pubsub // import "go.linka.cloud/pubsub"

import (
	"sync"
	"time"
)

var wgPool = sync.Pool{New: func() interface{} { return new(sync.WaitGroup) }}

// NewPublisher creates a new pub/sub publisher to broadcast messages.
// The duration is used as the send timeout as to not block the publisher publishing
// messages to other clients if one client is slow or unresponsive.
// The buffer is used when creating new channels for subscribers.
func NewPublisher[T any](publishTimeout time.Duration, buffer int) Publisher[T] {
	return &publisher[T]{
		buffer:      buffer,
		timeout:     publishTimeout,
		subscribers: make(map[subscriber[T]]topicFunc[T]),
	}
}

type subscriber[T any] chan T
type topicFunc[T any] func(v T) bool

// publisher is basic pub/sub structure. Allows to send events and subscribe
// to them. Can be safely used from multiple goroutines.
type publisher[T any] struct {
	m           sync.RWMutex
	buffer      int
	timeout     time.Duration
	subscribers map[subscriber[T]]topicFunc[T]
}

// Len returns the number of subscribers for the publisher
func (p *publisher[T]) Len() int {
	p.m.RLock()
	i := len(p.subscribers)
	p.m.RUnlock()
	return i
}

// Subscribe adds a new subscriber to the publisher returning the channel.
func (p *publisher[T]) Subscribe() chan T {
	return p.SubscribeTopic(nil)
}

// SubscribeTopic adds a new subscriber that filters messages sent by a topic.
func (p *publisher[T]) SubscribeTopic(topic topicFunc[T]) chan T {
	ch := make(chan T, p.buffer)
	p.m.Lock()
	p.subscribers[ch] = topic
	p.m.Unlock()
	return ch
}

// SubscribeTopicWithBuffer adds a new subscriber that filters messages sent by a topic.
// The returned channel has a buffer of the specified size.
func (p *publisher[T]) SubscribeTopicWithBuffer(topic topicFunc[T], buffer int) chan T {
	ch := make(chan T, buffer)
	p.m.Lock()
	p.subscribers[ch] = topic
	p.m.Unlock()
	return ch
}

// Evict removes the specified subscriber from receiving any more messages.
func (p *publisher[T]) Evict(sub chan T) {
	p.m.Lock()
	_, exists := p.subscribers[sub]
	if exists {
		delete(p.subscribers, sub)
		close(sub)
	}
	p.m.Unlock()
}

// Publish sends the data in v to all subscribers currently registered with the publisher.
func (p *publisher[T]) Publish(v T) {
	p.m.RLock()
	if len(p.subscribers) == 0 {
		p.m.RUnlock()
		return
	}

	wg := wgPool.Get().(*sync.WaitGroup)
	for sub, topic := range p.subscribers {
		wg.Add(1)
		go p.sendTopic(sub, topic, v, wg)
	}
	wg.Wait()
	wgPool.Put(wg)
	p.m.RUnlock()
}

// Close closes the channels to all subscribers registered with the publisher.
func (p *publisher[T]) Close() {
	p.m.Lock()
	for sub := range p.subscribers {
		delete(p.subscribers, sub)
		close(sub)
	}
	p.m.Unlock()
}

func (p *publisher[T]) sendTopic(sub subscriber[T], topic topicFunc[T], v T, wg *sync.WaitGroup) {
	defer wg.Done()
	if topic != nil && !topic(v) {
		return
	}

	// send under a select as to not block if the receiver is unavailable
	if p.timeout > 0 {
		timeout := time.NewTimer(p.timeout)
		defer timeout.Stop()

		select {
		case sub <- v:
		case <-timeout.C:
		}
		return
	}

	select {
	case sub <- v:
	default:
	}
}

type Publisher[T any] interface {
	Len() int
	Subscribe() chan T
	SubscribeTopic(topic topicFunc[T]) chan T
	SubscribeTopicWithBuffer(topic topicFunc[T], buffer int) chan T
	Evict(sub chan T)
	Publish(v T)
	Close()
}
