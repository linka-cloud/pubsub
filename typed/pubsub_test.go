package pubsub // import "github.com/docker/docker/pkg/pubsub"

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSendToOneSub(t *testing.T) {
	p := NewPublisher[string](100*time.Millisecond, 10)
	c := p.Subscribe()

	p.Publish("hi")

	msg := <-c
	if msg != "hi" {
		t.Fatalf("expected message hi but received %v", msg)
	}
}

func TestSendToMultipleSubs(t *testing.T) {
	p := NewPublisher[string](100*time.Millisecond, 10)
	var subs []chan string
	subs = append(subs, p.Subscribe(), p.Subscribe(), p.Subscribe())

	p.Publish("hi")

	for _, c := range subs {
		msg := <-c
		if msg != "hi" {
			t.Fatalf("expected message hi but received %v", msg)
		}
	}
}

func TestEvictOneSub(t *testing.T) {
	p := NewPublisher[string](100*time.Millisecond, 10)
	s1 := p.Subscribe()
	s2 := p.Subscribe()

	p.Evict(s1)
	p.Publish("hi")
	if _, ok := <-s1; ok {
		t.Fatal("expected s1 to not receive the published message")
	}

	msg := <-s2
	if msg != "hi" {
		t.Fatalf("expected message hi but received %v", msg)
	}
}

func TestClosePublisher(t *testing.T) {
	p := NewPublisher[string](100*time.Millisecond, 10)
	var subs []chan string
	subs = append(subs, p.Subscribe(), p.Subscribe(), p.Subscribe())
	p.Close()

	for _, c := range subs {
		if _, ok := <-c; ok {
			t.Fatal("expected all subscriber channels to be closed")
		}
	}
}

const sampleText = "test"

type testSubscriber struct {
	dataCh chan string
	ch     chan error
}

func (s *testSubscriber) Wait() error {
	return <-s.ch
}

func newTestSubscriber(p Publisher[string]) *testSubscriber {
	ts := &testSubscriber{
		dataCh: p.Subscribe(),
		ch:     make(chan error),
	}
	go func() {
		for s := range ts.dataCh {
			if s != sampleText {
				ts.ch <- fmt.Errorf("Unexpected text %s", s)
				break
			}
		}
		close(ts.ch)
	}()
	return ts
}

// for testing with -race
func TestPubSubRace(t *testing.T) {
	p := NewPublisher[string](0, 1024)
	var subs []*testSubscriber
	for j := 0; j < 50; j++ {
		subs = append(subs, newTestSubscriber(p))
	}
	for j := 0; j < 1000; j++ {
		p.Publish(sampleText)
	}
	time.AfterFunc(1*time.Second, func() {
		for _, s := range subs {
			p.Evict(s.dataCh)
		}
	})
	for _, s := range subs {
		s.Wait()
	}
}

func BenchmarkPubSubPublishNoSubscribers(b *testing.B) {
	p := NewPublisher[string](0, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Publish(sampleText)
	}
}

func BenchmarkPubSubPublishSubscribers(b *testing.B) {
	for _, n := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("subs=%d", n), func(b *testing.B) {
			p := NewPublisher[string](0, 1024)
			subs := make([]chan string, 0, n)
			for i := 0; i < n; i++ {
				subs = append(subs, p.Subscribe())
			}

			var wg sync.WaitGroup
			for _, sub := range subs {
				wg.Add(1)
				go func(ch chan string) {
					defer wg.Done()
					for range ch {
					}
				}(sub)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.Publish(sampleText)
			}
			b.StopTimer()

			p.Close()
			wg.Wait()
		})
	}
}

func BenchmarkPubSubPublishTopic(b *testing.B) {
	p := NewPublisher[string](0, 1024)
	accepted := p.SubscribeTopic(func(v string) bool {
		return len(v) > 0
	})
	rejected := p.SubscribeTopic(func(v string) bool {
		return len(v) == 0
	})

	var wg sync.WaitGroup
	for _, sub := range []chan string{accepted, rejected} {
		wg.Add(1)
		go func(ch chan string) {
			defer wg.Done()
			for range ch {
			}
		}(sub)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Publish(sampleText)
	}
	b.StopTimer()

	p.Close()
	wg.Wait()
}

func BenchmarkPubSubPublishTimeout(b *testing.B) {
	for _, n := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("subs=%d", n), func(b *testing.B) {
			p := NewPublisher[string](time.Nanosecond, 0)
			for i := 0; i < n; i++ {
				p.Subscribe()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.Publish(sampleText)
			}
			b.StopTimer()

			p.Close()
		})
	}
}
