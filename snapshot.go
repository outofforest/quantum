package quantum

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/photon"
)

// SpaceInfo stores information required to retrieve space.
type SpaceInfo struct {
	State   State
	Node    uint64
	HashMod uint64
}

// SnapshotInfo stores information required to retrieve snapshot.
type SnapshotInfo struct {
	SnapshotID   uint64
	SnapshotRoot SpaceInfo
	SpaceRoot    SpaceInfo
}

type spaceToCommit struct {
	HashMod      *uint64
	PInfo        ParentInfo
	OriginalItem uint64
}

// SnapshotConfig stores snapshot configuration.
type SnapshotConfig struct {
	SnapshotID uint64
	Allocator  *Allocator
}

// NewSnapshot creates new snapshot.
func NewSnapshot(config SnapshotConfig) (Snapshot, error) {
	if config.SnapshotID == 0 {
		_, node := config.Allocator.Allocate()
		snapshotInfo := photon.FromBytes[SnapshotInfo](node)
		*snapshotInfo = SnapshotInfo{}
	}

	snapshotInfo := *photon.FromBytes[SnapshotInfo](config.Allocator.Node(0))
	if config.SnapshotID > 0 && snapshotInfo.SnapshotID < config.SnapshotID-1 {
		return Snapshot{}, errors.Errorf("snapshot %d does not exist", config.SnapshotID)
	}

	if snapshotInfo.SnapshotID > config.SnapshotID {
		snapshots, err := NewSpace[uint64, SnapshotInfo](SpaceConfig{
			SnapshotID: config.SnapshotID,
			HashMod:    &snapshotInfo.SnapshotRoot.HashMod,
			Allocator:  config.Allocator,
			SpaceRoot: ParentInfo{
				State: lo.ToPtr(snapshotInfo.SnapshotRoot.State),
				Item:  lo.ToPtr[uint64](snapshotInfo.SnapshotRoot.Node),
			},
		})

		if err != nil {
			return Snapshot{}, err
		}

		var exists bool
		snapshotInfo, exists = snapshots.Get(config.SnapshotID)
		if !exists {
			return Snapshot{}, errors.Errorf("snapshot %d does not exist", config.SnapshotID)
		}
	}

	snapshots, err := NewSpace[uint64, SnapshotInfo](SpaceConfig{
		SnapshotID: config.SnapshotID,
		HashMod:    &snapshotInfo.SnapshotRoot.HashMod,
		Allocator:  config.Allocator,
		SpaceRoot: ParentInfo{
			State: lo.ToPtr(snapshotInfo.SnapshotRoot.State),
			Item:  lo.ToPtr(snapshotInfo.SnapshotRoot.Node),
		},
	})

	if err != nil {
		return Snapshot{}, err
	}

	spaces, err := NewSpace[uint64, SpaceInfo](SpaceConfig{
		SnapshotID: config.SnapshotID,
		HashMod:    &snapshotInfo.SpaceRoot.HashMod,
		Allocator:  config.Allocator,
		SpaceRoot: ParentInfo{
			State: lo.ToPtr(snapshotInfo.SpaceRoot.State),
			Item:  lo.ToPtr(snapshotInfo.SpaceRoot.Node),
		},
	})

	if err != nil {
		return Snapshot{}, err
	}

	s := Snapshot{
		config:         config,
		spaces:         spaces,
		snapshots:      snapshots,
		spacesToCommit: map[uint64]spaceToCommit{},
	}
	return s, nil
}

// Snapshot represents the state at particular point in time.
type Snapshot struct {
	config    SnapshotConfig
	spaces    *Space[uint64, SpaceInfo]
	snapshots *Space[uint64, SnapshotInfo]

	spacesToCommit map[uint64]spaceToCommit
}

// Space retrieves information about space.
func (s Snapshot) Space(spaceID uint64) SpaceInfo {
	spaceRootInfo, exists := s.spaces.Get(spaceID)
	if !exists {
		return SpaceInfo{}
	}

	return spaceRootInfo
}

// Commit commits current snapshot and returns next one.
func (s Snapshot) Commit() (Snapshot, error) {
	spaces := make([]uint64, 0, len(s.spacesToCommit))
	for spaceID := range s.spacesToCommit {
		spaces = append(spaces, spaceID)
	}
	sort.Slice(spaces, func(i, j int) bool { return spaces[i] < spaces[j] })

	for _, spaceID := range spaces {
		spaceToCommit := s.spacesToCommit[spaceID]
		if *spaceToCommit.PInfo.State == stateFree || *spaceToCommit.PInfo.Item == spaceToCommit.OriginalItem {
			continue
		}
		s.spaces.Set(spaceID, SpaceInfo{
			HashMod: *spaceToCommit.HashMod,
			State:   *spaceToCommit.PInfo.State,
			Node:    *spaceToCommit.PInfo.Item,
		})
	}

	clear(s.spacesToCommit)

	snapshotInfo := SnapshotInfo{
		SnapshotID: s.config.SnapshotID,
		SnapshotRoot: SpaceInfo{
			HashMod: *s.snapshots.config.HashMod,
			State:   *s.snapshots.config.SpaceRoot.State,
			Node:    *s.snapshots.config.SpaceRoot.Item,
		},
		SpaceRoot: SpaceInfo{
			HashMod: *s.spaces.config.HashMod,
			State:   *s.spaces.config.SpaceRoot.State,
			Node:    *s.spaces.config.SpaceRoot.Item,
		},
	}

	// FIXME (wojciech): Chicken and egg issue
	s.snapshots.Set(s.config.SnapshotID, snapshotInfo)
	snapshotInfo.SnapshotRoot = SpaceInfo{
		HashMod: *s.snapshots.config.HashMod,
		State:   *s.snapshots.config.SpaceRoot.State,
		Node:    *s.snapshots.config.SpaceRoot.Item,
	}
	s.snapshots.Set(s.config.SnapshotID, snapshotInfo)

	*photon.FromBytes[SnapshotInfo](s.config.Allocator.Node(0)) = snapshotInfo

	config := s.config
	config.SnapshotID = s.config.SnapshotID + 1

	return NewSnapshot(config)
}

// GetSpace retrieves space from snapshot.
func GetSpace[K, V comparable](spaceID uint64, snapshot Snapshot) (*Space[K, V], error) {
	space, exists := snapshot.spacesToCommit[spaceID]
	if !exists {
		spaceInfo := snapshot.Space(spaceID)
		space = spaceToCommit{
			HashMod: &spaceInfo.HashMod,
			PInfo: ParentInfo{
				State: &spaceInfo.State,
				Item:  &spaceInfo.Node,
			},
			OriginalItem: spaceInfo.Node,
		}
		snapshot.spacesToCommit[spaceID] = space
	}

	return NewSpace[K, V](SpaceConfig{
		SnapshotID: snapshot.config.SnapshotID,
		HashMod:    space.HashMod,
		Allocator:  snapshot.config.Allocator,
		SpaceRoot:  space.PInfo,
	})
}
