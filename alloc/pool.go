package alloc

import (
	"github.com/pkg/errors"
)

func NewPool[A Address](
	tapCh <-chan []A,
	sinkCh chan<- []A,
) *Pool[A] {
	pool := <-tapCh
	return &Pool[A]{
		tapCh:   tapCh,
		sinkCh:  sinkCh,
		pool:    pool,
		release: make([]A, 0, cap(pool)),
	}
}

type Pool[A Address] struct {
	tapCh  <-chan []A
	sinkCh chan<- []A

	// FIXME (wojciech): Leak of allocated addresses when pool object is abandoned
	pool    []A
	release []A
}

func (p *Pool[A]) Allocate() (A, error) {
	nodeAddress := p.pool[len(p.pool)-1]
	p.pool = p.pool[:len(p.pool)-1]

	if len(p.pool) == 0 {
		select {
		case p.pool = <-p.tapCh:
		default:
			return 0, errors.New("out of space")
		}
	}

	return nodeAddress, nil
}

func (p *Pool[A]) Deallocate(nodeAddress A) {
	if nodeAddress == 0 {
		return
	}

	p.release = append(p.release, nodeAddress)
	if len(p.release) == cap(p.release) {
		p.sinkCh <- p.release

		// FIXME (wojciech): Avoid heap allocation
		p.release = make([]A, 0, cap(p.release))
	}
}
