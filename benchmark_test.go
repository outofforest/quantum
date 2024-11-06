package quantum_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"os"
	"testing"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/logger"
	"github.com/outofforest/mass"
	"github.com/outofforest/parallel"
	"github.com/outofforest/quantum"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/persistent"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/tx"
	"github.com/outofforest/quantum/types"
)

// echo 120 | sudo tee /proc/sys/vm/nr_hugepages
// go test -benchtime=1x -bench=. -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out
// go test -c -o bench ./benchmark_test.go

func BenchmarkBalanceTransfer(b *testing.B) {
	const (
		spaceID        = 0x00
		numOfAddresses = 10_000_000
		txsPerCommit   = 20_000
		balance        = 100_000
	)

	b.StopTimer()
	b.ResetTimer()

	accounts := make([]tx.Account, 0, numOfAddresses)
	for range cap(accounts) {
		var address tx.Account
		_, _ = rand.Read(address[:])
		accounts = append(accounts, address)
	}

	for bi := 0; bi < b.N; bi++ {
		func() {
			fmt.Print("")
		}()
		func() {
			var size uint64 = 120 * 1024 * 1024 * 1024
			var nodeSize uint64 = 4 * 1024
			state, stateDeallocFunc, err := alloc.NewState(
				size,
				nodeSize,
				100,
				3,
				true,
				5,
			)
			if err != nil {
				panic(err)
			}
			defer stateDeallocFunc()

			//nolint:ineffassign,wastedassign,staticcheck
			stores, storesCloseFunc, err := fileStores([]string{
				"db0.quantum",
				// "db1.quantum",
				// "/tmp/d0/wojciech/db.quantum",
				// "/tmp/d1/wojciech/db.quantum",
			}, size, nodeSize)
			if err != nil {
				panic(err)
			}
			defer storesCloseFunc()

			stores = []persistent.Store{
				persistent.NewDummyStore(),
			}

			db, err := quantum.New(quantum.Config{
				State:  state,
				Stores: stores,
			})
			if err != nil {
				panic(err)
			}

			ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
			b.Cleanup(cancel)

			group := parallel.NewGroup(ctx)
			group.Spawn("db", parallel.Continue, db.Run)

			defer func() {
				group.Exit(nil)
				if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			}()

			defer func() {
				db.Close()
			}()

			volatilePool := db.NewVolatilePool()
			persistentPool := db.NewPersistentPool()

			s, err := quantum.GetSpace[tx.Account, tx.Balance](spaceID, db)
			if err != nil {
				panic(err)
			}

			pointerNode := s.NewPointerNode()
			dataNode := s.NewDataNode()

			func() {
				massTR := mass.New[pipeline.TransactionRequest](10)

				txRequest := pipeline.NewTransactionRequest(massTR)
				if err := s.AllocatePointers(txRequest, 3, volatilePool, pointerNode); err != nil {
					panic(err)
				}

				db.ApplyTransactionRequest(txRequest)

				for i := 0; i < numOfAddresses; i += 2 {
					txRequest := pipeline.NewTransactionRequest(massTR)

					v := s.Find(accounts[i], pointerNode)

					if err := v.Set(2*balance, txRequest, volatilePool, pointerNode, dataNode); err != nil {
						panic(err)
					}

					db.ApplyTransactionRequest(txRequest)
				}

				if err := db.Commit(volatilePool); err != nil {
					panic(err)
				}

				fmt.Println(s.Stats(pointerNode, dataNode))
				fmt.Println("===========================")

				for i := 0; i < numOfAddresses; i += 2 {
					v := s.Find(accounts[i], pointerNode)
					require.True(b, v.Exists(pointerNode, dataNode))
					require.Equal(b, tx.Balance(2*balance), v.Value(pointerNode, dataNode))
				}
			}()

			txIndex := 0
			var snapshotID types.SnapshotID = 1

			func() {
				b.StartTimer()
				for i := 0; i < numOfAddresses; i += 2 {
					db.ApplyTransaction(&tx.Transfer{
						From:   accounts[i],
						To:     accounts[i+1],
						Amount: balance,
					})

					txIndex++
					if txIndex%txsPerCommit == 0 {
						if err := db.Commit(volatilePool); err != nil {
							panic(err)
						}

						if snapshotID > 1 {
							if err := db.DeleteSnapshot(
								snapshotID-2,
								volatilePool,
								persistentPool,
							); err != nil {
								panic(err)
							}
						}
					}
				}

				if err := db.Commit(volatilePool); err != nil {
					panic(err)
				}

				b.StopTimer()
			}()

			func() {
				fmt.Println(s.Stats(pointerNode, dataNode))

				for _, addr := range accounts {
					v := s.Find(addr, pointerNode)
					require.True(b, v.Exists(pointerNode, dataNode))
					require.Equal(b, tx.Balance(balance), v.Value(pointerNode, dataNode))
				}
			}()
		}()
	}
}

func fileStores(
	paths []string,
	size uint64,
	nodeSize uint64,
) ([]persistent.Store, func(), error) {
	numOfStores := uint64(len(paths))
	stores := make([]persistent.Store, 0, numOfStores)
	funcs := make([]func(), 0, numOfStores)

	expectedNumOfNodes := size / nodeSize
	expectedCapacity := (expectedNumOfNodes + numOfStores - 1) / numOfStores * nodeSize
	seekTo := int64(expectedCapacity - nodeSize)
	data := make([]byte, 2*nodeSize-1)
	p := uint64(uintptr(unsafe.Pointer(&data[0])))
	p = (p+nodeSize-1)/nodeSize*nodeSize - p
	data = data[p : p+nodeSize]

	var minNumOfNodes uint64 = math.MaxUint64
	for _, path := range paths {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
		if err != nil {
			return nil, nil, err
		}

		fileSize, err := file.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		if fileSize == 0 {
			if _, err := file.Seek(seekTo, io.SeekEnd); err != nil {
				return nil, nil, errors.WithStack(err)
			}
			if _, err := file.Write(data); err != nil {
				return nil, nil, errors.WithStack(err)
			}
			fileSize = int64(expectedCapacity)
		}

		numOfNodes := uint64(fileSize) / nodeSize
		if numOfNodes < minNumOfNodes {
			minNumOfNodes = numOfNodes
		}

		store, storeCloseFunc, err := persistent.NewFileStore(file)
		if err != nil {
			return nil, nil, err
		}

		stores = append(stores, store)
		funcs = append(funcs, storeCloseFunc)
	}

	if minNumOfNodes*numOfStores < expectedNumOfNodes {
		return nil, nil, errors.New("files don't provide enough capacity")
	}

	return stores, func() {
		for _, f := range funcs {
			f()
		}
	}, nil
}
