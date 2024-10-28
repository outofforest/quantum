package space

import (
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/outofforest/quantum/types"
)

var collisions = [][]int{
	{15691551, 62234586, 76498628, 79645586},
	{6417226, 8828927, 78061179, 87384387},
	{9379853, 15271236, 26924827, 39742852},
	{71180670, 73568605, 96077640, 100118418},
	{11317952, 69053141, 82160848, 112455075},
	{33680651, 34881710, 52672514, 56033413},
	{635351, 7564491, 43998577, 77923294},
	{15069177, 60348274, 84185567, 116299206},
	{43622549, 93531002, 108158183, 115087013},
	{32134280, 33645087, 37005304, 83416269},
}

// go test -run=TestFindCollisions -v -tags=testing .

func TestFindCollisions(t *testing.T) {
	// Remove SkipNow and use command
	// go test -run=TestFindCollisions -v -tags=testing .
	// to generate integers with colliding hashes.
	t.SkipNow()

	fmt.Println("started")

	m := map[types.Hash][]int{}
	for i := range math.MaxInt {
		h := hashKey(i, nil, 0)
		if h2 := m[h]; len(h2) == 4 {
			sort.Ints(h2)
			fmt.Printf("%#v\n", h2)
		} else {
			m[h] = append(m[h], i)
		}
	}
}

func TestCollisions(t *testing.T) {
	for _, set := range collisions {
		m := map[types.Hash]struct{}{}
		for _, i := range set {
			m[hashKey(i, nil, 0)] = struct{}{}
		}
		assert.Len(t, m, 1)
	}
}
