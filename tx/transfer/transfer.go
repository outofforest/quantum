package transfer

import (
	"math"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space"
	txtypes "github.com/outofforest/quantum/tx/types"
	"github.com/outofforest/quantum/types"
)

// Tx defines transfer transaction.
type Tx struct {
	From   txtypes.Account
	To     txtypes.Account
	Amount txtypes.Amount

	from *space.Entry[txtypes.Account, txtypes.Amount]
	to   *space.Entry[txtypes.Account, txtypes.Amount]
}

// Prepare prepares transaction for execution.
func (t *Tx) Prepare(
	space *space.Space[txtypes.Account, txtypes.Amount],
	pointerNode *space.Node[types.Pointer],
) {
	t.from = space.Find(t.From, pointerNode)
	t.to = space.Find(t.To, pointerNode)
}

// Execute executes transaction.
func (t *Tx) Execute(
	tx *pipeline.TransactionRequest,
	volatilePool *alloc.Pool[types.VolatileAddress],
	pointerNode *space.Node[types.Pointer],
	dataNode *space.Node[types.DataItem[txtypes.Account, txtypes.Amount]],
) error {
	fromBalance := t.from.Value(pointerNode, dataNode)
	if fromBalance < t.Amount {
		return errors.Errorf("sender's balance is too low, balance: %d, amount to send: %d", fromBalance, t.Amount)
	}

	toBalance := t.to.Value(pointerNode, dataNode)
	if math.MaxUint64-toBalance < t.Amount {
		return errors.Errorf(
			"transfer cannot be executed because it would cause an overflow on the recipient's balance, balance: %d, amount to send: %d", //nolint:lll
			toBalance, t.Amount)
	}

	if err := t.from.Set(
		fromBalance-t.Amount,
		tx,
		volatilePool,
		pointerNode,
		dataNode,
	); err != nil {
		return err
	}

	return t.to.Set(
		toBalance+t.Amount,
		tx,
		volatilePool,
		pointerNode,
		dataNode,
	)
}