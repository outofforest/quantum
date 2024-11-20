package alloc

import (
	"github.com/pkg/errors"
)

// NewPool creates new allocation pool.
func NewPool[A Address](
	tapCh <-chan []A,
	sinkCh chan<- []A,
) *Pool[A] {
	pool := <-tapCh
	return &Pool[A]{
		tapCh:   tapCh,
		sinkCh:  sinkCh,
		pool:    pool,
		release: make([]A, 0, len(pool)),
	}
}

// Pool allocates and deallocates nodes in chunks.
type Pool[A Address] struct {
	tapCh  <-chan []A
	sinkCh chan<- []A

	pool    []A
	release []A
}

// Allocate allocates single node.
func (p *Pool[A]) Allocate() (A, error) {
	nodeAddress := p.pool[len(p.pool)-1]
	p.pool = p.pool[:len(p.pool)-1]

	if len(p.pool) == 0 {
		var ok bool
		if p.pool, ok = <-p.tapCh; !ok {
			return 0, errors.New("allocation failed")
		}
	}

	return nodeAddress, nil
}

// Deallocate deallocates single node.
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
