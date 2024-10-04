package quantum

import (
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func newDB(t *testing.T) *DB {
	requireT := require.New(t)

	db, err := New(Config{
		Allocator: NewAllocator(AllocatorConfig{
			TotalSize: 10 * 1024 * 1024,
			NodeSize:  512,
		}),
	})
	requireT.NoError(err)
	return db
}

const spaceID = 0x00

func TestCollisions(t *testing.T) {
	for _, set := range collisions {
		m := map[Hash]struct{}{}
		for _, i := range set {
			m[hashKey(i, 0)] = struct{}{}
		}
		assert.Len(t, m, 1)
	}
}

func TestSet(t *testing.T) {
	requireT := require.New(t)

	db := newDB(t)

	space, err := GetSpace[int, int](spaceID, db)
	requireT.NoError(err)

	for i := range 10 {
		requireT.NoError(space.Set(i, i))
	}

	requireT.Equal([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, collect(space))
}

func TestSetCollisions(t *testing.T) {
	requireT := require.New(t)

	db := newDB(t)

	space, err := GetSpace[int, int](spaceID, db)
	requireT.NoError(err)

	allValues := make([]int, 0, len(collisions)*len(collisions[0]))

	for _, set := range collisions {
		for _, i := range set {
			allValues = append(allValues, i)
			requireT.NoError(space.Set(i, i))
		}
	}

	sort.Ints(allValues)

	requireT.Equal(allValues, collect(space))
}

func TestGetCollisions(t *testing.T) {
	requireT := require.New(t)

	db := newDB(t)

	space, err := GetSpace[int, int](spaceID, db)
	requireT.NoError(err)

	inserted := make([]int, 0, len(collisions)*len(collisions[0]))
	read := make([]int, 0, len(collisions)*len(collisions[0]))

	for _, set := range collisions {
		for _, i := range set {
			inserted = append(inserted, i)
			requireT.NoError(space.Set(i, i))
		}
	}

	for _, set := range collisions {
		for _, i := range set {
			if v, exists := space.Get(i); exists {
				read = append(read, v)
			}
		}
	}

	sort.Ints(inserted)
	sort.Ints(read)

	requireT.Equal(inserted, read)
}

func TestSetOnNext(t *testing.T) {
	requireT := require.New(t)

	db := newDB(t)

	space1, err := GetSpace[int, int](spaceID, db)
	requireT.NoError(err)

	for i := range 10 {
		requireT.NoError(space1.Set(i, i))
	}

	requireT.NoError(db.Commit())

	space2, err := GetSpace[int, int](spaceID, db)
	requireT.NoError(err)

	for i := range 5 {
		requireT.NoError(space2.Set(i, i+10))
	}

	requireT.Equal([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, collect(space1))
	requireT.Equal([]int{5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, collect(space2))
}

func TestGet(t *testing.T) {
	requireT := require.New(t)

	db := newDB(t)

	space, err := GetSpace[int, int](spaceID, db)
	requireT.NoError(err)

	for i := range 10 {
		requireT.NoError(space.Set(i, i))
	}
	for i := range 10 {
		v, exists := space.Get(i)
		requireT.True(exists)
		requireT.Equal(i, v)
	}
}

func TestReplace(t *testing.T) {
	requireT := require.New(t)

	db := newDB(t)

	space1, err := GetSpace[int, int](spaceID, db)
	requireT.NoError(err)

	for i := range 10 {
		requireT.NoError(space1.Set(i, i))
	}

	requireT.NoError(db.Commit())

	space2, err := GetSpace[int, int](spaceID, db)
	requireT.NoError(err)

	for i, j := 0, 10; i < 5; i, j = i+1, j+1 {
		requireT.NoError(space2.Set(i, j))
	}

	for i := range 10 {
		v, exists := space1.Get(i)
		requireT.True(exists)
		requireT.Equal(i, v)
	}

	for i := range 5 {
		v, exists := space2.Get(i)
		requireT.True(exists)
		requireT.Equal(i+10, v)
	}

	for i := 5; i < 10; i++ {
		v, exists := space2.Get(i)
		requireT.True(exists)
		requireT.Equal(i, v)
	}
}

// go test -run=TestFindCollisions -v -tags=testing .

func TestFindCollisions(t *testing.T) {
	// Remove SkipNow and use command
	// go test -run=TestFindCollisions -v -tags=testing .
	// to generate integers with colliding hashes.
	t.SkipNow()

	fmt.Println("started")

	m := map[Hash][]int{}
	for i := range math.MaxInt {
		h := hashKey(i, 0)
		if h2 := m[h]; len(h2) == 4 {
			sort.Ints(h2)
			fmt.Printf("%#v\n", h2)
		} else {
			m[h] = append(m[h], i)
		}
	}
}

func collect(space *Space[int, int]) []int {
	values := []int{}
	typeStack := []State{*space.config.SpaceRoot.State}
	nodeStack := []NodeAddress{*space.config.SpaceRoot.Item}

	for {
		if len(nodeStack) == 0 {
			sort.Ints(values)
			return values
		}

		t := typeStack[len(typeStack)-1]
		n := nodeStack[len(nodeStack)-1]
		typeStack = typeStack[:len(typeStack)-1]
		nodeStack = nodeStack[:len(nodeStack)-1]

		switch t {
		case stateData:
			_, node := space.config.DataNodeAllocator.Get(n)
			for i := range len(node.Items) {
				if node.States[i] == stateData {
					values = append(values, node.Items[i].Value)
				}
			}
		default:
			_, node := space.config.PointerNodeAllocator.Get(n)
			for i := range len(node.Items) {
				if node.States[i] != stateFree {
					typeStack = append(typeStack, node.States[i])
					nodeStack = append(nodeStack, node.Items[i])
				}
			}
		}
	}
}
