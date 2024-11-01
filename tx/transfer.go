package tx

import (
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/queue"
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
	tx *queue.TransactionRequest,
	volatilePool *alloc.Pool[types.VolatileAddress],
	pointerNode *space.Node[types.Pointer],
	dataNode *space.Node[types.DataItem[Account, Balance]],
) error {
	// FIXME (wojciech): handle balance overflow.

	if err := t.from.Set(
		t.from.Value(pointerNode, dataNode)-t.Amount,
		tx,
		volatilePool,
		pointerNode,
		dataNode,
	); err != nil {
		return err
	}

	return t.to.Set(
		t.to.Value(pointerNode, dataNode)+t.Amount,
		tx,
		volatilePool,
		pointerNode,
		dataNode,
	)
}
