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
	OriginalItem NodeAddress
}

// SnapshotConfig stores snapshot configuration.
type SnapshotConfig struct {
	SnapshotID SnapshotID
	Allocator  *Allocator
}

// NewSnapshot creates new snapshot.
func NewSnapshot(config SnapshotConfig) (Snapshot, error) {
	if config.SnapshotID == 0 {
		config.Allocator.Allocate()
	}

	singularityNode := *photon.FromBytes[SingularityNode](config.Allocator.Node(0))
	if config.SnapshotID > 0 &&
		(singularityNode.SnapshotRoot.State == stateFree || singularityNode.SnapshotID < config.SnapshotID-1) {
		return Snapshot{}, errors.Errorf("snapshot %d does not exist", config.SnapshotID)
	}

	pointerNodeAllocator, err := NewSpaceNodeAllocator[NodeAddress](config.Allocator)
	if err != nil {
		return Snapshot{}, err
	}

	snapshotInfoNodeAllocator, err := NewSpaceNodeAllocator[DataItem[SnapshotID, SnapshotInfo]](config.Allocator)
	if err != nil {
		return Snapshot{}, err
	}

	spaceInfoNodeAllocator, err := NewSpaceNodeAllocator[DataItem[SpaceID, SpaceInfo]](config.Allocator)
	if err != nil {
		return Snapshot{}, err
	}

	snapshotToNodeNodeAllocator, err := NewSpaceNodeAllocator[DataItem[SnapshotID, NodeAddress]](config.Allocator)
	if err != nil {
		return Snapshot{}, err
	}

	listNodeAllocator, err := NewListNodeAllocator(config.Allocator)
	if err != nil {
		return Snapshot{}, err
	}

	snapshots := NewSpace[SnapshotID, SnapshotInfo](SpaceConfig[SnapshotID, SnapshotInfo]{
		SnapshotID: config.SnapshotID,
		HashMod:    &singularityNode.SnapshotRoot.HashMod,
		SpaceRoot: ParentInfo{
			State: lo.ToPtr(singularityNode.SnapshotRoot.State),
			Item:  lo.ToPtr(singularityNode.SnapshotRoot.Node),
		},
		PointerNodeAllocator: pointerNodeAllocator,
		DataNodeAllocator:    snapshotInfoNodeAllocator,
	})

	var snapshotInfo SnapshotInfo
	if singularityNode.SnapshotRoot.State != stateFree {
		snapshotID := config.SnapshotID
		if singularityNode.SnapshotID == snapshotID-1 {
			snapshotID = singularityNode.SnapshotID
		}

		var exists bool
		snapshotInfo, exists = snapshots.Get(snapshotID)
		if !exists {
			return Snapshot{}, errors.Errorf("snapshot %d does not exist", config.SnapshotID)
		}
	}

	deallocator := NewDeallocator(
		config.SnapshotID,
		NewSpace[SnapshotID, NodeAddress](SpaceConfig[SnapshotID, NodeAddress]{
			SnapshotID: config.SnapshotID,
			HashMod:    &snapshotInfo.DeallocationRoot.HashMod,
			SpaceRoot: ParentInfo{
				State: lo.ToPtr(snapshotInfo.DeallocationRoot.State),
				Item:  lo.ToPtr(snapshotInfo.DeallocationRoot.Node),
			},
			PointerNodeAllocator: pointerNodeAllocator,
			DataNodeAllocator:    snapshotToNodeNodeAllocator,
		}),
		listNodeAllocator,
	)
	snapshots.config.Deallocator = deallocator
	deallocator.deallocationLists.config.Deallocator = deallocator

	spaces := NewSpace[SpaceID, SpaceInfo](SpaceConfig[SpaceID, SpaceInfo]{
		SnapshotID: config.SnapshotID,
		HashMod:    &snapshotInfo.SpaceRoot.HashMod,
		SpaceRoot: ParentInfo{
			State: lo.ToPtr(snapshotInfo.SpaceRoot.State),
			Item:  lo.ToPtr(snapshotInfo.SpaceRoot.Node),
		},
		PointerNodeAllocator: pointerNodeAllocator,
		DataNodeAllocator:    spaceInfoNodeAllocator,
		Deallocator:          deallocator,
	})

	s := Snapshot{
		config:         config,
		snapshots:      snapshots,
		spaces:         spaces,
		deallocator:    deallocator,
		spacesToCommit: map[SpaceID]spaceToCommit{},
	}
	return s, nil
}

// Snapshot represents the state at particular point in time.
type Snapshot struct {
	config      SnapshotConfig
	snapshots   *Space[SnapshotID, SnapshotInfo]
	spaces      *Space[SpaceID, SpaceInfo]
	deallocator Deallocator

	spacesToCommit map[SpaceID]spaceToCommit
}

// Space retrieves information about space.
func (s Snapshot) Space(spaceID SpaceID) SpaceInfo {
	spaceRootInfo, exists := s.spaces.Get(spaceID)
	if !exists {
		return SpaceInfo{}
	}

	return spaceRootInfo
}

// Commit commits current snapshot and returns next one.
func (s Snapshot) Commit() (Snapshot, error) {
	spaces := make([]SpaceID, 0, len(s.spacesToCommit))
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

	s.snapshots.Set(s.config.SnapshotID, SnapshotInfo{
		SnapshotID: s.config.SnapshotID,
		DeallocationRoot: SpaceInfo{
			HashMod: *s.deallocator.deallocationLists.config.HashMod,
			State:   *s.deallocator.deallocationLists.config.SpaceRoot.State,
			Node:    *s.deallocator.deallocationLists.config.SpaceRoot.Item,
		},
		SpaceRoot: SpaceInfo{
			HashMod: *s.spaces.config.HashMod,
			State:   *s.spaces.config.SpaceRoot.State,
			Node:    *s.spaces.config.SpaceRoot.Item,
		},
	})

	*photon.FromBytes[SingularityNode](s.config.Allocator.Node(0)) = SingularityNode{
		SnapshotID: s.config.SnapshotID,
		SnapshotRoot: SpaceInfo{
			HashMod: *s.snapshots.config.HashMod,
			State:   *s.snapshots.config.SpaceRoot.State,
			Node:    *s.snapshots.config.SpaceRoot.Item,
		},
	}

	config := s.config
	config.SnapshotID = s.config.SnapshotID + 1

	return NewSnapshot(config)
}

// GetSpace retrieves space from snapshot.
func GetSpace[K, V comparable](spaceID SpaceID, snapshot Snapshot) (*Space[K, V], error) {
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

	pointerNodeAllocator, err := NewSpaceNodeAllocator[NodeAddress](snapshot.config.Allocator)
	if err != nil {
		return nil, err
	}

	dataNodeAllocator, err := NewSpaceNodeAllocator[DataItem[K, V]](snapshot.config.Allocator)
	if err != nil {
		return nil, err
	}

	return NewSpace[K, V](SpaceConfig[K, V]{
		SnapshotID:           snapshot.config.SnapshotID,
		HashMod:              space.HashMod,
		SpaceRoot:            space.PInfo,
		PointerNodeAllocator: pointerNodeAllocator,
		DataNodeAllocator:    dataNodeAllocator,
		Deallocator:          snapshot.deallocator,
	}), nil
}
