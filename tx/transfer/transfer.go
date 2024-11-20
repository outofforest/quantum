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
func (t *Tx) Prepare(space *space.Space[txtypes.Account, txtypes.Amount], hashBuff []byte, hashMatches []uint64) {
	t.from = space.Find(t.From, hashBuff, hashMatches)
	t.to = space.Find(t.To, hashBuff, hashMatches)
}

// Execute executes transaction.
func (t *Tx) Execute(
	tx *pipeline.TransactionRequest,
	volatilePool *alloc.Pool[types.VolatileAddress],
	hashBuff []byte,
	hashMatches []uint64,
) error {
	fromBalance := t.from.Value(hashBuff, hashMatches)
	if fromBalance < t.Amount {
		return errors.Errorf("sender's balance is too low, balance: %d, amount to send: %d", fromBalance, t.Amount)
	}

	toBalance := t.to.Value(hashBuff, hashMatches)
	if math.MaxUint64-toBalance < t.Amount {
		return errors.Errorf(
			"transfer cannot be executed because it would cause an overflow on the recipient's balance, balance: %d, amount to send: %d", //nolint:lll
			toBalance, t.Amount)
	}

	if err := t.from.Set(
		fromBalance-t.Amount,
		tx,
		volatilePool,
		hashBuff,
		hashMatches,
	); err != nil {
		return err
	}

	return t.to.Set(
		toBalance+t.Amount,
		tx,
		volatilePool,
		hashBuff,
		hashMatches,
	)
}
