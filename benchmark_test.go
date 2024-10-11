package quantum

import (
	"crypto/rand"
	"testing"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// go test -benchtime=100x -bench=. -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

func BenchmarkBalanceTransfer(b *testing.B) {
	const (
		spaceID        = 0x00
		numOfAddresses = 10000
		txsPerBlock    = 1000
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
		db, err := New(Config{
			Allocator: alloc.NewAllocator(alloc.Config{
				TotalSize: 40 * 1024 * 1024,
				NodeSize:  4 * 1024,
			}),
		})
		if err != nil {
			panic(err)
		}

		s, err := GetSpace[accountAddress, accountBalance](spaceID, db)
		if err != nil {
			panic(err)
		}

		for i := 0; i < numOfAddresses; i += 2 {
			if err := s.Set(accounts[i], balance); err != nil {
				panic(err)
			}
		}

		tx := 0
		var snapshotID types.SnapshotID

		b.StartTimer()
		for i := 0; i < numOfAddresses; i += 2 {
			senderAddress := accounts[i]
			recipientAddress := accounts[i+1]

			senderBalance, _ := s.Get(senderAddress)
			recipientBalance, _ := s.Get(recipientAddress)

			senderBalance -= balance
			recipientBalance += balance

			if err := s.Set(senderAddress, senderBalance); err != nil {
				panic(err)
			}
			if err := s.Set(recipientAddress, recipientBalance); err != nil {
				panic(err)
			}

			tx++
			if tx%txsPerBlock == 0 {
				_ = db.Commit()
				snapshotID++

				if snapshotID > 1 {
					if err := db.DeleteSnapshot(snapshotID - 2); err != nil {
						panic(err)
					}
				}

				var err error
				s, err = GetSpace[accountAddress, accountBalance](spaceID, db)
				if err != nil {
					panic(err)
				}
			}
		}
		b.StopTimer()
	}
}

type accountAddress [20]byte
type accountBalance uint64
