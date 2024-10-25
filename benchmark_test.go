package quantum_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/outofforest/quantum"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/persistent"
	"github.com/outofforest/quantum/types"
)

// echo 70 | sudo tee /proc/sys/vm/nr_hugepages
// go test -benchtime=1x -bench=. -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out
// go test -c -o bench ./benchmark_test.go

func BenchmarkBalanceTransfer(b *testing.B) {
	const (
		spaceID        = 0x00
		numOfAddresses = 50_000_000
		txsPerCommit   = 2000
		balance        = 100_000
	)

	b.StopTimer()
	b.ResetTimer()

	accounts := make([]accountAddress, 0, numOfAddresses)
	for range cap(accounts) {
		var address accountAddress
		_, _ = rand.Read(address[:])
		accounts = append(accounts, address)
	}

	for bi := 0; bi < b.N; bi++ {
		func() {
			var size uint64 = 70 * 1024 * 1024 * 1024
			state, stateDeallocFunc, err := alloc.NewState(
				size,
				4*1024,
				100,
				3,
				true,
				5,
			)
			if err != nil {
				panic(err)
			}
			defer stateDeallocFunc()

			file, err := os.OpenFile("db.quantum", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
			if err != nil {
				panic(err)
			}

			if _, err := file.Seek(int64(size-1), io.SeekStart); err != nil {
				panic(err)
			}
			if _, err := file.Write([]byte{0x00}); err != nil {
				panic(err)
			}

			var store persistent.Store
			var storeCloseFunc func()

			//nolint:ineffassign,wastedassign
			store, storeCloseFunc, err = persistent.NewFileStore(file, size)
			if err != nil {
				panic(err)
			}
			defer storeCloseFunc()

			store = persistent.NewDummyStore()

			db, err := quantum.New(quantum.Config{
				State: state,
				Store: store,
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

			defer db.Close()

			volatilePool := db.NewVolatilePool()
			persistentPool := db.NewPersistentPool()

			s, err := quantum.GetSpace[accountAddress, accountBalance](spaceID, db)
			if err != nil {
				panic(err)
			}

			pointerNode := s.NewPointerNode()
			dataNode := s.NewDataNode()

			if err := s.AllocatePointers(3, volatilePool, pointerNode); err != nil {
				panic(err)
			}

			for i := 0; i < numOfAddresses; i += 2 {
				v := s.Find(accounts[i], pointerNode, dataNode)

				if err := v.Set(2*balance, volatilePool, pointerNode, dataNode); err != nil {
					panic(err)
				}

				// v = s.Find(accounts[i])
				// require.Equal(b, accountBalance(2*balance), v.Value())
			}

			fmt.Println(s.Stats(pointerNode))
			fmt.Println("===========================")

			tx := 0
			var snapshotID types.SnapshotID

			if err := db.Commit(volatilePool, persistentPool); err != nil {
				panic(err)
			}

			snapshotID++

			func() {
				b.StartTimer()
				for i := 0; i < numOfAddresses; i += 2 {
					senderAddress := accounts[i]
					recipientAddress := accounts[i+1]

					senderBalance := s.Find(senderAddress, pointerNode, dataNode)
					recipientBalance := s.Find(recipientAddress, pointerNode, dataNode)

					if err := senderBalance.Set(
						senderBalance.Value()-balance,
						volatilePool,
						pointerNode,
						dataNode,
					); err != nil {
						panic(err)
					}
					if err := recipientBalance.Set(
						recipientBalance.Value()+balance,
						volatilePool,
						pointerNode,
						dataNode,
					); err != nil {
						panic(err)
					}

					tx++
					if tx%txsPerCommit == 0 {
						if err := db.Commit(volatilePool, persistentPool); err != nil {
							panic(err)
						}
						snapshotID++

						if snapshotID > 1 {
							if err := db.DeleteSnapshot(snapshotID-2, volatilePool, persistentPool); err != nil {
								panic(err)
							}
						}
					}
				}
				b.StopTimer()
			}()

			fmt.Println(s.Stats(pointerNode))

			for _, addr := range accounts {
				require.Equal(b, accountBalance(balance), s.Find(addr, pointerNode, dataNode).Value())
			}
		}()
	}
}

type accountAddress [20]byte
type accountBalance uint64
