package transfer

import (
	"math"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space"
	txtypes "github.com/outofforest/quantum/tx/types"
	"github.com/outofforest/quantum/types"
	"github.com/outofforest/quantum/wal"
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
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	hashBuff []byte, hashMatches []uint64,
) error {
	if err := s.Find(&t.from, snapshotID, tx, walRecorder, allocator, t.From, space.StagePointer0, hashBuff,
		hashMatches); err != nil {
		return err
	}

	return s.Find(&t.to, snapshotID, tx, walRecorder, allocator, t.To, space.StagePointer0, hashBuff, hashMatches)
}

// Execute executes transaction.
func (t *Tx) Execute(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	hashBuff []byte,
	hashMatches []uint64,
) error {
	fromBalance, err := t.from.Value(snapshotID, tx, walRecorder, allocator, hashBuff, hashMatches)
	if err != nil {
		return err
	}
	if fromBalance < t.Amount {
		return errors.Errorf("sender's balance is too low, balance: %d, amount to send: %d", fromBalance, t.Amount)
	}

	toBalance, err := t.to.Value(snapshotID, tx, walRecorder, allocator, hashBuff, hashMatches)
	if err != nil {
		return err
	}
	if math.MaxUint64-toBalance < t.Amount {
		return errors.Errorf(
			"transfer cannot be executed because it would cause an overflow on the recipient's balance, balance: %d, amount to send: %d", //nolint:lll
			toBalance, t.Amount)
	}

	if err := t.from.Set(
		snapshotID,
		tx,
		walRecorder,
		allocator,
		fromBalance-t.Amount,
		hashBuff,
		hashMatches,
	); err != nil {
		return err
	}

	return t.to.Set(
		snapshotID,
		tx,
		walRecorder,
		allocator,
		toBalance+t.Amount,
		hashBuff,
		hashMatches,
	)
}
