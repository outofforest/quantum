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
	space *space.Space[txtypes.Account, txtypes.Amount],
	tx *pipeline.TransactionRequest,
	volatilePool *alloc.Pool[types.VolatileAddress],
) error {
	for _, a := range t.Accounts {
		if err := space.Find(a.Account).Set(
			a.Amount,
			tx,
			volatilePool,
		); err != nil {
			return err
		}
	}

	return nil
}
