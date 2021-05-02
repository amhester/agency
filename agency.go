package agency

import (
	"context"
	"sync"
)

type empty struct{}

// Pool a utility for managing and synchronizing work across a bounded number of goroutines.
type Pool struct {
	parent      *Pool
	cancel      func()
	wg          sync.WaitGroup
	errOnce     sync.Once
	err         error
	semaphore   chan empty
	maxRoutines int
}

// New returns a new instance of an agency Pool bounded by the given number of max goroutines.
func New(ctx context.Context, maxRoutines int) (*Pool, context.Context) {
	cancelCtx, cancel := context.WithCancel(ctx)
	sema := make(chan empty, maxRoutines)
	pool := &Pool{
		cancel:      cancel,
		wg:          sync.WaitGroup{},
		semaphore:   sema,
		maxRoutines: maxRoutines,
	}
	for i := 0; i < maxRoutines; i++ {
		sema <- empty{}
	}
	return pool, cancelCtx
}

func (p *Pool) error(err error) {
	p.err = err
}

func (p *Pool) acquire() {
	if p.parent != nil {
		p.parent.acquire()
	}
	<-p.semaphore
}

func (p *Pool) tryAcquire() bool {
	if p.parent != nil {
		ok := p.parent.tryAcquire()
		if !ok {
			return false
		}
	}

	select {
	case <-p.semaphore:
	default:
		return false
	}

	return true
}

func (p *Pool) release() {
	if p.parent != nil {
		p.parent.release()
	}
	p.semaphore <- empty{}
}

// Wait blocks until all currently running routines are finished, returning any error returned from the worker functions. Use Wait if you plan to reuse this Pool.
func (p *Pool) Wait() error {
	p.wg.Wait()
	if p.cancel != nil {
		p.cancel()
	}
	retErr := p.err
	p.err = nil
	return retErr
}

// Go accepts worker function which takes no arguments and may return an error. This function will block until there routines available to handle the function.
// Any error returned from the passed in worker function will be returned in the Wait or Close call.
func (p *Pool) Go(f func() error) {
	p.acquire()
	if p.err != nil {
		p.release()
		return
	}
	p.wg.Add(1)
	go func() {
		defer p.release()
		defer p.wg.Done()

		if err := f(); err != nil {
			p.errOnce.Do(func() {
				p.error(err)
				if p.cancel != nil {
					p.cancel()
				}
			})
		}
	}()
}

// TryGo will attempt to run a worker function if there is a routine available, otherwise it will return false. This function is non-blocking.
func (p *Pool) TryGo(f func() error) bool {
	ok := p.tryAcquire()
	if !ok {
		return false
	}
	if p.err != nil {
		p.release()
		return false
	}
	p.wg.Add(1)
	go func() {
		defer p.release()
		defer p.wg.Done()

		if err := f(); err != nil {
			p.errOnce.Do(func() {
				p.error(err)
				if p.cancel != nil {
					p.cancel()
				}
			})
		}
	}()
	return true
}

// Close will wait for all routines to finish, returning any errors returned during execution and will close all internal communication channels.
// After calling Close, the pool becomes unusable.
func (p *Pool) Close() error {
	if err := p.Wait(); err != nil {
		return err
	}
	close(p.semaphore)
	return nil
}

// Child creates a child pool that is bounded both by the passed in number of max routines as well as its parent's routine pool.
// This allows you to create a global pool or bounds and create smaller pools based on some global maximum.
func (p *Pool) Child(ctx context.Context, maxRoutines int) (*Pool, context.Context) {
	child, childCtx := New(ctx, maxRoutines)
	child.parent = p
	return child, childCtx
}
