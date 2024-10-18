package quantum

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// go test -benchtime=1x -bench=. -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

func BenchmarkBalanceTransfer(b *testing.B) {
	const (
		spaceID        = 0x00
		numOfAddresses = 10_000_000
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
		allocator, deallocFunc, err := alloc.NewAllocator(alloc.Config{
			TotalSize:    20 * 1024 * 1024 * 1024,
			NodeSize:     4 * 1024,
			UseHugePages: true,
		})
		if err != nil {
			panic(err)
		}
		db, err := New(Config{
			Allocator: allocator,
		})
		if err != nil {
			panic(err)
		}

		s, err := GetSpace[accountAddress, accountBalance](spaceID, db)
		if err != nil {
			panic(err)
		}
		if err := s.AllocatePointers(3); err != nil {
			panic(err)
		}

		for i := 0; i < numOfAddresses; i += 2 {
			v := s.Get(accounts[i])
			_, err := v.Set(2 * balance)
			if err != nil {
				panic(err)
			}

			// v = s.Get(accounts[i])
			// require.Equal(b, accountBalance(2*balance), v.Value())
		}

		fmt.Println(s.Stats())
		fmt.Println("===========================")

		tx := 0
		var snapshotID types.SnapshotID

		_ = db.Commit()
		snapshotID++

		func() {
			b.StartTimer()
			for i := 0; i < numOfAddresses; i += 2 {
				senderAddress := accounts[i]
				recipientAddress := accounts[i+1]

				senderBalance := s.Get(senderAddress)
				recipientBalance := s.Get(recipientAddress)

				if _, err := senderBalance.Set(senderBalance.Value() - balance); err != nil {
					panic(err)
				}
				if _, err := recipientBalance.Set(recipientBalance.Value() + balance); err != nil {
					panic(err)
				}

				tx++
				if tx%txsPerCommit == 0 {
					_ = db.Commit()
					snapshotID++

					if snapshotID > 1 {
						if err := db.DeleteSnapshot(snapshotID - 2); err != nil {
							panic(err)
						}
					}
				}
			}
			b.StopTimer()

			fmt.Println(s.Stats())
		}()

		for _, addr := range accounts {
			require.Equal(b, accountBalance(balance), s.Get(addr).Value())
		}

		deallocFunc()
	}
}

type accountAddress [20]byte
type accountBalance uint64
