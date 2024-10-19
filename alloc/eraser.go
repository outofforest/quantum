package alloc

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/outofforest/parallel"
	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

// RunEraser runs the goroutine erasing deallocated nodes.
func RunEraser(
	ctx context.Context,
	tapCh <-chan []types.LogicalAddress,
	sinkCh chan<- []types.LogicalAddress,
	nodeSize uint64,
	state *State,
	numOfWorkers uint64,
) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		for i := range numOfWorkers {
			spawn(fmt.Sprintf("worker-%02d", i), parallel.Fail, func(ctx context.Context) error {
				for nodes := range tapCh {
					for _, n := range nodes {
						clear(photon.SliceFromPointer[byte](state.Node(n), int(nodeSize)))
					}
					sinkCh <- nodes
				}

				return errors.WithStack(ctx.Err())
			})
		}
		return nil
	})
}
