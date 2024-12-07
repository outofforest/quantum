package genesis

import (
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space"
	txtypes "github.com/outofforest/quantum/tx/types"
	"github.com/outofforest/quantum/types"
)

// InitialBalance represents initial balance for an account set in genesis.
type InitialBalance struct {
	Account txtypes.Account
	Amount  txtypes.Amount
}

// Tx defines genesis transaction.
type Tx struct {
	Accounts []InitialBalance
}

// Execute executes transaction.
func (t *Tx) Execute(
	s *space.Space[txtypes.Account, txtypes.Amount],
	tx *pipeline.TransactionRequest,
	allocator *alloc.Allocator[types.VolatileAddress],
	hashBuff []byte,
	hashMatches []uint64,
) error {
	for _, a := range t.Accounts {
		var v space.Entry[txtypes.Account, txtypes.Amount]
		s.Find(&v, a.Account, space.StageData, hashBuff, hashMatches)

		if err := v.Set(
			tx,
			allocator,
			a.Amount,
			hashBuff,
			hashMatches,
		); err != nil {
			return err
		}
	}

	return nil
}
