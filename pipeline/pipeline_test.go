package pipeline

import (
	"context"
	"testing"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestTransactionFactoryNewTransaction(t *testing.T) {
	requireT := require.New(t)

	txFactory := NewTransactionRequestFactory()
	tx := txFactory.New()

	requireT.Nil(tx.Transaction)
	requireT.Nil(tx.StoreRequest)
	requireT.Equal(&tx.StoreRequest, tx.LastStoreRequest)
	requireT.Nil(tx.ListRequest)
	requireT.Equal(&tx.ListRequest, tx.LastListRequest)
	requireT.Nil(tx.CommitCh)
	requireT.Nil(tx.Next)
	requireT.Equal(None, tx.Type)
}

func TestTransactionAddStoreRequest(t *testing.T) {
	requireT := require.New(t)

	txFactory := NewTransactionRequestFactory()
	tx := txFactory.New()

	sr1 := &StoreRequest{}
	tx.AddStoreRequest(sr1)

	requireT.Equal(sr1, tx.StoreRequest)
	requireT.Equal(&sr1.Next, tx.LastStoreRequest)
	requireT.Nil(sr1.Next)
	requireT.Nil(tx.Transaction)
	requireT.Nil(tx.ListRequest)
	requireT.Equal(&tx.ListRequest, tx.LastListRequest)
	requireT.Nil(tx.CommitCh)
	requireT.Nil(tx.Next)
	requireT.Equal(None, tx.Type)

	sr2 := &StoreRequest{}
	tx.AddStoreRequest(sr2)

	requireT.Equal(sr1, tx.StoreRequest)
	requireT.Equal(&sr2.Next, tx.LastStoreRequest)
	requireT.Equal(sr2, sr1.Next)
	requireT.Nil(sr2.Next)
	requireT.Nil(tx.Transaction)
	requireT.Nil(tx.ListRequest)
	requireT.Equal(&tx.ListRequest, tx.LastListRequest)
	requireT.Nil(tx.CommitCh)
	requireT.Nil(tx.Next)
	requireT.Equal(None, tx.Type)
}

func TestTransactionAddListRequest(t *testing.T) {
	requireT := require.New(t)

	txFactory := NewTransactionRequestFactory()
	tx := txFactory.New()

	lr1 := &ListRequest{}
	tx.AddListRequest(lr1)

	requireT.Equal(lr1, tx.ListRequest)
	requireT.Equal(&lr1.Next, tx.LastListRequest)
	requireT.Nil(lr1.Next)
	requireT.Nil(tx.Transaction)
	requireT.Nil(tx.StoreRequest)
	requireT.Equal(&tx.StoreRequest, tx.LastStoreRequest)
	requireT.Nil(tx.CommitCh)
	requireT.Nil(tx.Next)
	requireT.Equal(None, tx.Type)

	lr2 := &ListRequest{}
	tx.AddListRequest(lr2)

	requireT.Equal(lr1, tx.ListRequest)
	requireT.Equal(&lr2.Next, tx.LastListRequest)
	requireT.Equal(lr2, lr1.Next)
	requireT.Nil(lr2.Next)
	requireT.Nil(tx.Transaction)
	requireT.Nil(tx.StoreRequest)
	requireT.Equal(&tx.StoreRequest, tx.LastStoreRequest)
	requireT.Nil(tx.CommitCh)
	requireT.Nil(tx.Next)
	requireT.Equal(None, tx.Type)
}

func TestPipelinePush(t *testing.T) {
	requireT := require.New(t)

	p, r := New()
	txFactory := NewTransactionRequestFactory()

	requireT.NotNil(p.availableCount)
	requireT.Equal(uint64(0), *p.availableCount)
	requireT.Equal(uint64(0), p.count)

	requireT.NotNil(r.head)
	requireT.Equal(p.tail, r.head)
	requireT.Equal([]*uint64{p.availableCount}, r.availableCounts)

	tx1 := txFactory.New()
	p.Push(tx1)

	requireT.Equal(tx1.Next, *p.tail)
	requireT.Equal(uint64(0), *p.availableCount)
	requireT.Equal(uint64(1), p.count)
	requireT.Equal(tx1, *r.head)

	tx2 := txFactory.New()
	p.Push(tx2)

	requireT.Equal(tx2.Next, *p.tail)
	requireT.Equal(uint64(0), *p.availableCount)
	requireT.Equal(uint64(2), p.count)
	requireT.Equal(tx1, *r.head)
}

func TestPipelinePushMany(t *testing.T) {
	requireT := require.New(t)

	p, _ := New()
	txFactory := NewTransactionRequestFactory()

	for range atomicDivider {
		p.Push(txFactory.New())
	}

	requireT.Equal(uint64(atomicDivider), *p.availableCount)
	requireT.Equal(uint64(atomicDivider), p.count)
}

func TestPipelinePushOnCommit(t *testing.T) {
	requireT := require.New(t)

	p, _ := New()
	txFactory := NewTransactionRequestFactory()

	tx := txFactory.New()
	tx.Type = Commit
	p.Push(tx)

	requireT.Equal(uint64(1), *p.availableCount)
	requireT.Equal(uint64(1), p.count)
}

func TestPipelinePushOnClose(t *testing.T) {
	requireT := require.New(t)

	p, _ := New()
	txFactory := NewTransactionRequestFactory()

	tx := txFactory.New()
	tx.Type = Close
	p.Push(tx)

	requireT.Equal(uint64(1), *p.availableCount)
	requireT.Equal(uint64(1), p.count)
}

func TestReaderRead(t *testing.T) {
	requireT := require.New(t)

	p, r := New()
	txFactory := NewTransactionRequestFactory()

	tx1 := txFactory.New()
	p.Push(tx1)

	requireT.Equal(uint64(0), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.processedCount)

	tx2 := txFactory.New()
	tx2.Type = Commit
	p.Push(tx2)

	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.processedCount)

	tx, readCount, err := r.Read(context.Background())
	requireT.NoError(err)
	requireT.Equal(uint64(1), readCount)
	requireT.Equal(tx1, tx)
	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.processedCount)

	tx, readCount, err = r.Read(context.Background())
	requireT.NoError(err)
	requireT.Equal(uint64(2), readCount)
	requireT.Equal(tx2, tx)
	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.processedCount)

	r.Acknowledge(1, tx1.Type)
	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.processedCount)

	r.Acknowledge(2, tx2.Type)
	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(2), *r.processedCount)
}

func TestChildReaderRead(t *testing.T) {
	requireT := require.New(t)

	p, r1 := New()
	txFactory := NewTransactionRequestFactory()

	r2 := NewReader(r1)

	tx1 := txFactory.New()
	p.Push(tx1)

	tx2 := txFactory.New()
	tx2.Type = Commit
	p.Push(tx2)

	requireT.Equal(uint64(2), *r1.availableCounts[0])
	requireT.Equal(uint64(0), *r1.processedCount)

	requireT.Equal(uint64(0), *r2.availableCounts[0])
	requireT.Equal(uint64(0), *r2.processedCount)

	_, _, err := r1.Read(context.Background())
	requireT.NoError(err)

	_, _, err = r1.Read(context.Background())
	requireT.NoError(err)

	r1.Acknowledge(2, tx2.Type)
	requireT.Equal(uint64(2), *r1.availableCounts[0])
	requireT.Equal(uint64(2), *r1.processedCount)

	requireT.Equal(uint64(2), *r2.availableCounts[0])
	requireT.Equal(uint64(0), *r2.processedCount)

	tx, readCount, err := r2.Read(context.Background())
	requireT.NoError(err)
	requireT.Equal(uint64(1), readCount)
	requireT.Equal(tx1, tx)
	requireT.Equal(uint64(2), *r2.availableCounts[0])
	requireT.Equal(uint64(0), *r2.processedCount)

	tx, readCount, err = r2.Read(context.Background())
	requireT.NoError(err)
	requireT.Equal(uint64(2), readCount)
	requireT.Equal(tx2, tx)
	requireT.Equal(uint64(2), *r2.availableCounts[0])
	requireT.Equal(uint64(0), *r2.processedCount)

	r2.Acknowledge(1, tx1.Type)
	requireT.Equal(uint64(2), *r2.availableCounts[0])
	requireT.Equal(uint64(0), *r2.processedCount)

	r2.Acknowledge(2, tx2.Type)
	requireT.Equal(uint64(2), *r2.availableCounts[0])
	requireT.Equal(uint64(2), *r2.processedCount)
}

func TestChildReaderReadWithTwoParents(t *testing.T) {
	requireT := require.New(t)

	p, rp1 := New()
	txFactory := NewTransactionRequestFactory()

	rp2 := CloneReader(rp1)

	r := NewReader(rp1, rp2)

	tx1 := txFactory.New()
	p.Push(tx1)

	tx2 := txFactory.New()
	tx2.Type = Commit
	p.Push(tx2)

	requireT.Equal(uint64(2), *rp1.availableCounts[0])
	requireT.Equal(uint64(0), *rp1.processedCount)

	requireT.Equal(uint64(2), *rp2.availableCounts[0])
	requireT.Equal(uint64(0), *rp2.processedCount)

	requireT.Equal(uint64(0), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.availableCounts[1])
	requireT.Equal(uint64(0), *r.processedCount)

	_, _, err := rp1.Read(context.Background())
	requireT.NoError(err)

	_, _, err = rp1.Read(context.Background())
	requireT.NoError(err)

	rp1.Acknowledge(2, tx2.Type)
	requireT.Equal(uint64(2), *rp1.availableCounts[0])
	requireT.Equal(uint64(2), *rp1.processedCount)

	requireT.Equal(uint64(2), *rp2.availableCounts[0])
	requireT.Equal(uint64(0), *rp2.processedCount)

	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.availableCounts[1])
	requireT.Equal(uint64(0), *r.processedCount)

	rp2.Acknowledge(2, tx2.Type)
	requireT.Equal(uint64(2), *rp2.availableCounts[0])
	requireT.Equal(uint64(2), *rp2.processedCount)

	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(2), *r.availableCounts[1])
	requireT.Equal(uint64(0), *r.processedCount)

	tx, readCount, err := r.Read(context.Background())
	requireT.NoError(err)
	requireT.Equal(uint64(1), readCount)
	requireT.Equal(tx1, tx)
	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(2), *r.availableCounts[1])
	requireT.Equal(uint64(0), *r.processedCount)

	tx, readCount, err = r.Read(context.Background())
	requireT.NoError(err)
	requireT.Equal(uint64(2), readCount)
	requireT.Equal(tx2, tx)
	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(2), *r.availableCounts[1])
	requireT.Equal(uint64(0), *r.processedCount)

	r.Acknowledge(1, tx1.Type)
	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(2), *r.availableCounts[1])
	requireT.Equal(uint64(0), *r.processedCount)

	r.Acknowledge(2, tx2.Type)
	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(2), *r.availableCounts[1])
	requireT.Equal(uint64(2), *r.processedCount)
}

func TestChildReaderReadAcknowledgmentFromOneParentIsNotEnough(t *testing.T) {
	requireT := require.New(t)

	p, rp1 := New()
	txFactory := NewTransactionRequestFactory()

	rp2 := CloneReader(rp1)

	r := NewReader(rp1, rp2)

	tx1 := txFactory.New()
	p.Push(tx1)

	tx2 := txFactory.New()
	tx2.Type = Commit
	p.Push(tx2)

	requireT.Equal(uint64(0), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.availableCounts[1])
	requireT.Equal(uint64(0), *r.processedCount)

	_, _, err := rp1.Read(context.Background())
	requireT.NoError(err)

	_, _, err = rp1.Read(context.Background())
	requireT.NoError(err)

	rp1.Acknowledge(2, tx2.Type)

	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.availableCounts[1])
	requireT.Equal(uint64(0), *r.processedCount)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	t.Cleanup(cancel)

	_, _, err = r.Read(ctx)
	requireT.ErrorIs(err, context.DeadlineExceeded)

	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.availableCounts[1])
	requireT.Equal(uint64(0), *r.processedCount)
}

func TestReaderClone(t *testing.T) {
	requireT := require.New(t)

	p, r1 := New()

	txFactory := NewTransactionRequestFactory()

	tx := txFactory.New()
	tx.Type = Close
	p.Push(tx)

	requireT.Equal(uint64(1), *r1.availableCounts[0])
	requireT.Equal(uint64(0), *r1.processedCount)

	r2 := CloneReader(r1)

	requireT.NotEqual(uintptr(unsafe.Pointer(r1)), uintptr(unsafe.Pointer(r2)))
	requireT.Equal(r1.head, r2.head)
	requireT.Equal(r1.availableCounts, r2.availableCounts)
	requireT.Equal(uint64(0), *r2.processedCount)

	tx, readCount, err := r1.Read(context.Background())
	requireT.NoError(err)
	r1.Acknowledge(readCount, tx.Type)

	requireT.Equal(uint64(1), *r1.availableCounts[0])
	requireT.Equal(uint64(1), *r1.processedCount)
	requireT.Equal(uint64(1), *r2.availableCounts[0])
	requireT.Equal(uint64(0), *r2.processedCount)
}

func TestReaderRun(t *testing.T) {
	requireT := require.New(t)

	p, r := New()
	txFactory := NewTransactionRequestFactory()

	tx1 := txFactory.New()
	p.Push(tx1)

	tx2 := txFactory.New()
	tx2.Type = Commit
	p.Push(tx2)

	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.processedCount)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	t.Cleanup(cancel)

	txs := map[uint64]*TransactionRequest{}
	err := r.Run(ctx, func(tx *TransactionRequest, count uint64) (uint64, error) {
		txs[count] = tx
		return count, nil
	})

	requireT.ErrorIs(err, context.DeadlineExceeded)
	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(2), *r.processedCount)
	requireT.Len(txs, 2)
	requireT.Equal(tx1, txs[1])
	requireT.Equal(tx2, txs[2])
}

func TestReaderRunWithError(t *testing.T) {
	requireT := require.New(t)

	p, r := New()
	txFactory := NewTransactionRequestFactory()

	tx1 := txFactory.New()
	p.Push(tx1)

	tx2 := txFactory.New()
	tx2.Type = Commit
	p.Push(tx2)

	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.processedCount)

	testErr := errors.New("test")
	txs := map[uint64]*TransactionRequest{}
	err := r.Run(context.Background(), func(tx *TransactionRequest, count uint64) (uint64, error) {
		txs[count] = tx
		return count, testErr
	})

	requireT.ErrorIs(err, testErr)
	requireT.Equal(uint64(2), *r.availableCounts[0])
	requireT.Equal(uint64(0), *r.processedCount)
	requireT.Len(txs, 1)
	requireT.Equal(tx1, txs[1])
}
