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

	from space.Entry[txtypes.Account, txtypes.Amount]
	to   space.Entry[txtypes.Account, txtypes.Amount]
}

// Prepare prepares transaction for execution.
func (t *Tx) Prepare(
	s *space.Space[txtypes.Account, txtypes.Amount],
	hashBuff []byte, hashMatches []uint64,
) {
	s.Find(&t.from, t.From, space.StagePointer0, hashBuff, hashMatches)
	s.Find(&t.to, t.To, space.StagePointer0, hashBuff, hashMatches)
}

// Execute executes transaction.
func (t *Tx) Execute(
	s *space.Space[txtypes.Account, txtypes.Amount],
	tx *pipeline.TransactionRequest,
	allocator *alloc.Allocator[types.VolatileAddress],
	hashBuff []byte,
	hashMatches []uint64,
) error {
	fromBalance := s.ReadKey(&t.from, hashBuff, hashMatches)
	if fromBalance < t.Amount {
		return errors.Errorf("sender's balance is too low, balance: %d, amount to send: %d", fromBalance, t.Amount)
	}

	toBalance := s.ReadKey(&t.to, hashBuff, hashMatches)
	if math.MaxUint64-toBalance < t.Amount {
		return errors.Errorf(
			"transfer cannot be executed because it would cause an overflow on the recipient's balance, balance: %d, amount to send: %d", //nolint:lll
			toBalance, t.Amount)
	}

	fromBalance -= t.Amount
	toBalance += t.Amount

	if fromBalance == 0 {
		s.DeleteKey(&t.from, tx, hashBuff, hashMatches)
	} else {
		if err := s.SetKey(&t.from, tx, allocator, fromBalance, hashBuff, hashMatches); err != nil {
			return err
		}
	}

	if toBalance == 0 {
		s.DeleteKey(&t.to, tx, hashBuff, hashMatches)
	}

	return s.SetKey(&t.to, tx, allocator, toBalance, hashBuff, hashMatches)
}
