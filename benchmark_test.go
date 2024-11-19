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
	"github.com/outofforest/parallel"
	"github.com/outofforest/quantum"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/persistent"
	"github.com/outofforest/quantum/tx/genesis"
	"github.com/outofforest/quantum/tx/transfer"
	txtypes "github.com/outofforest/quantum/tx/types"
	"github.com/outofforest/quantum/types"
)

// echo 120 | sudo tee /proc/sys/vm/nr_hugepages
// go test -benchtime=1x -timeout=24h -bench=. -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out
// go test -c -o bench ./benchmark_test.go

func BenchmarkBalanceTransfer(b *testing.B) {
	const (
		spaceID        = 0x00
		numOfAddresses = 20_000_000
		txsPerCommit   = 20_000
		balance        = 100_000
	)

	b.StopTimer()
	b.ResetTimer()

	var accounts [numOfAddresses]txtypes.Account
	accountBytes := unsafe.Slice(&accounts[0][0], unsafe.Sizeof(accounts))

	// f, err := os.Open("accounts")
	// require.NoError(b, err)
	// _, err = f.Read(accountBytes)
	// require.NoError(b, err)
	// require.NoError(b, f.Close())

	defer func() {
		if b.Failed() {
			_ = os.WriteFile("accounts", accountBytes, 0o600)
		}
	}()

	for bi := 0; bi < b.N; bi++ {
		func() {
			_, _ = rand.Read(accountBytes)

			var size uint64 = 120 * 1024 * 1024 * 1024
			state, stateDeallocFunc, err := alloc.NewState(
				size,
				100,
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
			}, size)
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

			s, err := quantum.GetSpace[txtypes.Account, txtypes.Amount](spaceID, db)
			if err != nil {
				panic(err)
			}

			hashMatches := s.NewHashMatches()

			func() {
				db.ApplyTransaction(&genesis.Tx{
					Accounts: []genesis.InitialBalance{
						{
							Account: txtypes.GenesisAccount,
							Amount:  numOfAddresses * balance,
						},
					},
				})
				if err := db.Commit(); err != nil {
					panic(err)
				}

				fmt.Println(s.Stats())
				fmt.Println("===========================")

				v := s.Find(txtypes.GenesisAccount, hashMatches)
				require.True(b, v.Exists(hashMatches))
				require.Equal(b, txtypes.Amount(numOfAddresses*balance), v.Value(hashMatches))
			}()

			txIndex := 0
			var snapshotID types.SnapshotID = 1

			func() {
				b.StartTimer()
				for i := range numOfAddresses {
					db.ApplyTransaction(&transfer.Tx{
						From:   txtypes.GenesisAccount,
						To:     accounts[i],
						Amount: balance,
					})

					txIndex++
					if txIndex%txsPerCommit == 0 {
						if err := db.Commit(); err != nil {
							panic(err)
						}

						if snapshotID > 1 {
							db.DeleteSnapshot(snapshotID - 2)
						}
					}
				}

				if err := db.Commit(); err != nil {
					panic(err)
				}

				b.StopTimer()
			}()

			func() {
				fmt.Println(s.Stats())

				v := s.Find(txtypes.GenesisAccount, hashMatches)
				require.True(b, v.Exists(hashMatches))
				require.Equal(b, txtypes.Amount(0), v.Value(hashMatches))

				for _, addr := range accounts {
					v := s.Find(addr, hashMatches)
					require.True(b, v.Exists(hashMatches))
					require.Equal(b, txtypes.Amount(balance), v.Value(hashMatches))
				}
			}()
		}()
	}
}

func fileStores(
	paths []string,
	size uint64,
) ([]persistent.Store, func(), error) {
	numOfStores := uint64(len(paths))
	stores := make([]persistent.Store, 0, numOfStores)
	funcs := make([]func(), 0, numOfStores)

	expectedNumOfNodes := size / types.NodeLength
	expectedCapacity := (expectedNumOfNodes + numOfStores - 1) / numOfStores * types.NodeLength
	seekTo := int64(expectedCapacity - types.NodeLength)
	data := make([]byte, 2*types.NodeLength-1)
	p := uint64(uintptr(unsafe.Pointer(&data[0])))
	p = (p+types.NodeLength-1)/types.NodeLength*types.NodeLength - p
	data = data[p : p+types.NodeLength]

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

		numOfNodes := uint64(fileSize) / types.NodeLength
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
