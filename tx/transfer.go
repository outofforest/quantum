package tx

import (
	"math"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/types"
)

// BalanceSpaceID specifies space ID used to store balances.
const BalanceSpaceID = 0x00

// Account defines the type for account address.
type Account [20]byte

// Balance defines type for balance.
type Balance uint64

// Transfer defines transfer transaction.
type Transfer struct {
	From   Account
	To     Account
	Amount Balance

	from *space.Entry[Account, Balance]
	to   *space.Entry[Account, Balance]
}

// Prepare prepares transaction for execution.
func (t *Transfer) Prepare(
	space *space.Space[Account, Balance],
	pointerNode *space.Node[types.Pointer],
) {
	t.from = space.Find(t.From, pointerNode)
	t.to = space.Find(t.To, pointerNode)
}

// Execute executes transaction.
func (t *Transfer) Execute(
	tx *pipeline.TransactionRequest,
	volatilePool *alloc.Pool[types.VolatileAddress],
	pointerNode *space.Node[types.Pointer],
	dataNode *space.Node[types.DataItem[Account, Balance]],
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
