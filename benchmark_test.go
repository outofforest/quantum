package quantum_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/outofforest/quantum"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/persistent"
	"github.com/outofforest/quantum/tx/genesis"
	"github.com/outofforest/quantum/tx/transfer"
	txtypes "github.com/outofforest/quantum/tx/types"
	"github.com/outofforest/quantum/tx/types/spaces"
	"github.com/outofforest/quantum/types"
)

// echo 50 | sudo tee /proc/sys/vm/nr_hugepages
// go test -benchtime=1x -timeout=24h -bench=. -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out
// go test -c -o bench ./benchmark_test.go

// * soft memlock unlimited
// * hard memlock unlimited

func BenchmarkBalanceTransfer(b *testing.B) {
	const (
		numOfAddresses = 50_000_000
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

			store, err := fileStore(
				// "./db.quantum",
				"./disk",
			)
			if err != nil {
				panic(err)
			}

			var size uint64 = 10 * 1024 * 1024 * 1024
			state, stateDeallocFunc, err := alloc.NewState(
				size, store.Size(),
				true,
			)
			if err != nil {
				panic(err)
			}
			defer stateDeallocFunc()

			db := quantum.New(quantum.Config{
				State: state,
				Store: store,
			})

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

			s, err := quantum.GetSpace[txtypes.Account, txtypes.Amount](spaces.Balances, db)
			if err != nil {
				panic(err)
			}

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

				genesisBalance, genesisExists := s.Query(txtypes.GenesisAccount)
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

				genesisBalance, genesisExists := s.Query(txtypes.GenesisAccount)
				require.False(b, genesisExists)
				require.Equal(b, txtypes.Amount(0), genesisBalance)

				for _, addr := range accounts {
					accBalance, accExists := s.Query(addr)
					require.True(b, accExists)
					require.Equal(b, txtypes.Amount(balance), accBalance)
				}
			}()
		}()
	}
}

func fileStore(path string) (*persistent.Store, error) {
	file, err := os.OpenFile(path, os.O_RDWR|unix.O_DIRECT, 0o600)
	if err != nil {
		return nil, err
	}

	return persistent.NewStore(file)
}
