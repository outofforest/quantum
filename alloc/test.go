package alloc

import (
	"context"
	"testing"

	"github.com/pkg/errors"

	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
)

// RunInTest creates and runs state for unit tests.
func RunInTest(t *testing.T, size, nodesPerGroup uint64) (*State, error) {
	state, stateDeallocFunc, err := NewState(
		size, size,
		nodesPerGroup,
		false,
	)
	if err != nil {
		return nil, err
	}
	t.Cleanup(stateDeallocFunc)

	ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
	t.Cleanup(cancel)

	group := parallel.NewGroup(ctx)
	group.Spawn("state", parallel.Continue, state.Run)

	t.Cleanup(func() {
		state.Close()
		group.Exit(nil)
		if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})

	return state, nil
}
