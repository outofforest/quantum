package pipeline

import (
	"testing"

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
