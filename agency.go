package agency

import (
	"context"
	"sync"
)

type empty struct{}

type Pool struct {
	parent      *Pool
	cancel      func()
	wg          sync.WaitGroup
	errOnce     sync.Once
	err         error
	semaphore   chan empty
	maxRoutines int
}

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

func (p *Pool) Wait() error {
	p.wg.Wait()
	if p.cancel != nil {
		p.cancel()
	}
	retErr := p.err
	p.err = nil
	return retErr
}

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

func (p *Pool) Close() error {
	if err := p.Wait(); err != nil {
		return err
	}
	close(p.semaphore)
	return nil
}

func (p *Pool) Child(ctx context.Context, maxRoutines int) (*Pool, context.Context) {
	child, childCtx := New(ctx, maxRoutines)
	child.parent = p
	return child, childCtx
}
