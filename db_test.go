package quantum

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/space"
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
		Allocator: alloc.NewAllocator(alloc.Config{
			TotalSize: 10 * 1024 * 1024,
			NodeSize:  512,
		}),
	})
	requireT.NoError(err)
	return db
}

const spaceID = 0x00

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

func collect(space *space.Space[int, int]) []int {
	values := []int{}
	for item := range space.Iterator() {
		values = append(values, item.Value)
	}

	sort.Ints(values)
	return values
}
