package quantum_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
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

// echo 100 | sudo tee /proc/sys/vm/nr_hugepages
// go test -benchtime=1x -timeout=24h -bench=. -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out
// go test -c -o bench ./benchmark_test.go

func BenchmarkBalanceTransfer(b *testing.B) {
	const (
		spaceID        = 0x00
		numOfAddresses = 5_000_000
		txsPerCommit   = 20_000
		balance        = 100_000
	)

	b.StopTimer()
	b.ResetTimer()

	accounts := make([]txtypes.Account, numOfAddresses)
	accountBytes := unsafe.Slice(&accounts[0][0], uintptr(len(accounts))*unsafe.Sizeof(txtypes.Account{}))

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

			var size uint64 = 20 * 1024 * 1024 * 1024
			state, stateDeallocFunc, err := alloc.NewState(
				size,
				100,
				true,
			)
			if err != nil {
				panic(err)
			}
			defer stateDeallocFunc()

			//nolint:ineffassign,wastedassign,staticcheck
			store, err := fileStore(
				// "/tmp/d0/wojciech/db.quantum",
				"db0.quantum",
				size)
			if err != nil {
				panic(err)
			}

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

			defer func() {
				db.Close()
			}()

			s, err := quantum.GetSpace[txtypes.Account, txtypes.Amount](spaceID, db)
			if err != nil {
				panic(err)
			}

			hashBuff := s.NewHashBuff()
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

				genesisBalance, genesisExists := s.Query(txtypes.GenesisAccount, hashBuff, hashMatches)
				require.True(b, genesisExists)
				require.Equal(b, txtypes.Amount(numOfAddresses*balance), genesisBalance)
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
						snapshotID++

						if snapshotID > 1 {
							db.DeleteSnapshot(snapshotID - 1)
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

				genesisBalance, genesisExists := s.Query(txtypes.GenesisAccount, hashBuff, hashMatches)
				require.True(b, genesisExists)
				require.Equal(b, txtypes.Amount(0), genesisBalance)

				for _, addr := range accounts {
					accBalance, accExists := s.Query(addr, hashBuff, hashMatches)
					require.True(b, accExists)
					require.Equal(b, txtypes.Amount(balance), accBalance)
				}
			}()
		}()
	}
}

func fileStore(path string, size uint64) (persistent.Store, error) {
	expectedNumOfNodes := size / types.NodeLength
	expectedCapacity := expectedNumOfNodes * types.NodeLength
	seekTo := int64(expectedCapacity - types.NodeLength)
	data := make([]byte, 2*types.NodeLength-1)
	p := uint64(uintptr(unsafe.Pointer(&data[0])))
	p = (p+types.NodeLength-1)/types.NodeLength*types.NodeLength - p
	data = data[p : p+types.NodeLength]

	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}

	if _, err := file.Seek(seekTo, io.SeekEnd); err != nil {
		return nil, errors.WithStack(err)
	}
	if _, err := file.Write(data); err != nil {
		return nil, errors.WithStack(err)
	}

	return persistent.NewFileStore(file)
}
