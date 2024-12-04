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
	s *space.Space[txtypes.Account, txtypes.Amount],
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	hashBuff []byte,
	hashMatches []uint64,
) error {
	for _, a := range t.Accounts {
		var v space.Entry[txtypes.Account, txtypes.Amount]
		if err := s.Find(&v, snapshotID, tx, walRecorder, allocator, a.Account, space.StageData, hashBuff,
			hashMatches); err != nil {
			return err
		}

		if err := v.Set(
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
