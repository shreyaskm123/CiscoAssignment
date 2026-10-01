package app

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"streamer/internal/mqclient"

	mqpb "streamer/proto"
)

// batcher buffers owned events and flushes them to the MQ publish stream when
// either the batch is full or the flush interval elapses. Sending in batches
// lowers per-message overhead while the flush interval bounds latency.
//
// Graceful shutdown contract:
//   - closeInput() stops accepting new events.
//   - drain(grace) returns after every event that was already accepted has
//     been written to the MQ stream (or after grace), so buffered/in-progress
//     events are delivered before the process exits.
type batcher struct {
	size     int
	interval time.Duration
	pub      *mqclient.Stream

	ch       chan *mqpb.Event
	done     chan struct{}
	closing  chan struct{}
	closeOne sync.Once
	produced atomic.Int64
	acked    atomic.Int64

	// Send outcomes, for Status(). lastSendOK/lastSendFail are unix nanos:
	// zero means "never happened", which is what a replica that has not managed
	// to publish anything at all looks like.
	lastSendOK   atomic.Int64
	lastSendFail atomic.Int64
}

func newBatcher(size int, interval time.Duration, pub *mqclient.Stream) *batcher {
	if size < 1 {
		size = 1
	}
	return &batcher{
		size:     size,
		interval: interval,
		pub:      pub,
		ch:       make(chan *mqpb.Event, size),
		done:     make(chan struct{}),
		closing:  make(chan struct{}),
	}
}

// start launches the flush goroutine.
func (b *batcher) start(ctx context.Context) {
	go b.loop(ctx)
}

// add enqueues an event for delivery. Once closeInput has been called, adds are
// dropped (they belong to a stream that is already shutting down). The select
// makes the drop/accept decision atomic with respect to closeInput so produced
// always equals the number of events the drain is responsible for.
func (b *batcher) add(e *mqpb.Event) {
	select {
	case <-b.closing:
		return
	case b.ch <- e:
		b.produced.Add(1)
	}
}

// closeInput flips the input gate so no further events are accepted.
func (b *batcher) closeInput() {
	b.closeOne.Do(func() { close(b.closing) })
}

// drain waits until the flush goroutine has delivered every accepted event AND
// the MQ has acknowledged them, or until grace elapses (bounded shutdown when
// the MQ is unreachable). Waiting for acks is what guarantees no data loss:
// gRPC Send() only buffers locally, so cancelling before acks arrive would
// drop already-"sent" events.
func (b *batcher) drain(grace time.Duration) {
	b.closeInput()
	deadline := time.Now().Add(grace)

	select {
	case <-b.done:
	case <-time.After(grace):
		log.Printf("batcher drain timed out after %s; %d events still buffered", grace, b.pendingLen())
		return
	}

	for b.pub.Acked() < b.pub.Sent() {
		if time.Now().After(deadline) {
			log.Printf("batcher drain: %d sent events still unacked after %s",
				b.pub.Sent()-b.pub.Acked(), grace)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// pendingLen reports events accepted but not yet fully delivered.
func (b *batcher) pendingLen() int64 {
	return b.produced.Load() - b.acked.Load()
}

func (b *batcher) loop(ctx context.Context) {
	defer close(b.done)

	t := time.NewTicker(b.interval)
	defer t.Stop()

	var pending []*mqpb.Event
	flush := func() {
		if len(pending) == 0 {
			return
		}
		for _, e := range pending {
			if err := b.pub.Send(e); err != nil {
				log.Printf("publish %s: %v", e.EventId, err)
				b.lastSendFail.Store(time.Now().UnixNano())
				return
			}
			b.lastSendOK.Store(time.Now().UnixNano())
			b.acked.Add(1)
		}
		pending = pending[:0]
	}
	// drainAll writes every event still sitting on b.ch into the flush passed
	// to it. Only safe after closeInput: producers have stopped.
	drainAll := func() {
		for {
			select {
			case e := <-b.ch:
				pending = append(pending, e)
			default:
				return
			}
		}
	}

	for {
		select {
		case <-ctx.Done(): // run loop cancelled: deliver everything buffered
			b.closeInput()
			drainAll()
			flushDrain := func() {
				if len(pending) == 0 {
					return
				}
				deadline := time.Now().Add(2 * time.Second)
				for _, e := range pending {
					if err := b.pub.SendUntil(e, deadline); err != nil {
						log.Printf("drop %s during drain: %v", e.EventId, err)
						return
					}
					b.acked.Add(1)
				}
				pending = pending[:0]
			}
			flushDrain()
			_ = b.pub.CloseSend()
			return
		case <-t.C:
			flush()
		case e := <-b.ch:
			pending = append(pending, e)
			if len(pending) >= b.size {
				flush()
			}
		}
	}
}
