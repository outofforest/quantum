package genesis

import (
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space"
	txtypes "github.com/outofforest/quantum/tx/types"
	"github.com/outofforest/quantum/types"
	"github.com/outofforest/quantum/wal"
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
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	hashBuff []byte,
	hashMatches []uint64,
) error {
	for _, a := range t.Accounts {
		v, err := space.Find(snapshotID, tx, walRecorder, allocator, a.Account, hashBuff, hashMatches)
		if err != nil {
			return err
		}

		if err := v.Set(
			snapshotID,
			tx,
			walRecorder,
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
