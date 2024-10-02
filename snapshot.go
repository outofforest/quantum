package quantum

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/photon"
)

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
		config.Allocator.Allocate()
	}

	singularityNode := *photon.FromBytes[SingularityNode](config.Allocator.Node(0))
	if config.SnapshotID > 0 && singularityNode.SnapshotID < config.SnapshotID-1 {
		return Snapshot{}, errors.Errorf("snapshot %d does not exist", config.SnapshotID)
	}

	pointerNodeAllocator, err := NewNodeAllocator[uint64](config.Allocator)
	if err != nil {
		return Snapshot{}, err
	}

	snapshotInfoNodeAllocator, err := NewNodeAllocator[DataItem[uint64, SnapshotInfo]](config.Allocator)
	if err != nil {
		return Snapshot{}, err
	}

	var snapshotInfo SnapshotInfo
	if singularityNode.SnapshotRoot.State != stateFree {
		snapshots, err := NewSpace[uint64, SnapshotInfo](SpaceConfig[uint64, SnapshotInfo]{
			SnapshotID: config.SnapshotID,
			HashMod:    &singularityNode.SnapshotRoot.HashMod,
			SpaceRoot: ParentInfo{
				State: lo.ToPtr(singularityNode.SnapshotRoot.State),
				Item:  lo.ToPtr[uint64](singularityNode.SnapshotRoot.Node),
			},
			PointerNodeAllocator: pointerNodeAllocator,
			DataNodeAllocator:    snapshotInfoNodeAllocator,
		})

		if err != nil {
			return Snapshot{}, err
		}

		snapshotID := config.SnapshotID
		if singularityNode.SnapshotID == snapshotID-1 {
			// FIXME (wojciech): In other cases snapshot should be read-only.
			snapshotID = singularityNode.SnapshotID
		}

		var exists bool
		snapshotInfo, exists = snapshots.Get(snapshotID)
		if !exists {
			return Snapshot{}, errors.Errorf("snapshot %d does not exist", config.SnapshotID)
		}
	}

	snapshots, err := NewSpace[uint64, SnapshotInfo](SpaceConfig[uint64, SnapshotInfo]{
		SnapshotID: config.SnapshotID,
		HashMod:    &snapshotInfo.SnapshotRoot.HashMod,
		SpaceRoot: ParentInfo{
			State: lo.ToPtr(snapshotInfo.SnapshotRoot.State),
			Item:  lo.ToPtr(snapshotInfo.SnapshotRoot.Node),
		},
		PointerNodeAllocator: pointerNodeAllocator,
		DataNodeAllocator:    snapshotInfoNodeAllocator,
	})

	if err != nil {
		return Snapshot{}, err
	}

	spaceInfoNodeAllocator, err := NewNodeAllocator[DataItem[uint64, SpaceInfo]](config.Allocator)
	if err != nil {
		return Snapshot{}, err
	}

	spaces, err := NewSpace[uint64, SpaceInfo](SpaceConfig[uint64, SpaceInfo]{
		SnapshotID: config.SnapshotID,
		HashMod:    &snapshotInfo.SpaceRoot.HashMod,
		SpaceRoot: ParentInfo{
			State: lo.ToPtr(snapshotInfo.SpaceRoot.State),
			Item:  lo.ToPtr(snapshotInfo.SpaceRoot.Node),
		},
		PointerNodeAllocator: pointerNodeAllocator,
		DataNodeAllocator:    spaceInfoNodeAllocator,
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

	*photon.FromBytes[SingularityNode](s.config.Allocator.Node(0)) = SingularityNode{
		SnapshotID:   snapshotInfo.SnapshotID,
		SnapshotRoot: snapshotInfo.SnapshotRoot,
	}

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

	pointerNodeAllocator, err := NewNodeAllocator[uint64](snapshot.config.Allocator)
	if err != nil {
		return nil, err
	}

	dataNodeAllocator, err := NewNodeAllocator[DataItem[K, V]](snapshot.config.Allocator)
	if err != nil {
		return nil, err
	}

	return NewSpace[K, V](SpaceConfig[K, V]{
		SnapshotID:           snapshot.config.SnapshotID,
		HashMod:              space.HashMod,
		SpaceRoot:            space.PInfo,
		PointerNodeAllocator: pointerNodeAllocator,
		DataNodeAllocator:    dataNodeAllocator,
	})
}
