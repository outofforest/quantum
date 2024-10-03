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
	// FIXME (wojciech): If it's the already-committed snapshot, mark it as read-only.

	if config.SnapshotID == 0 {
		config.Allocator.Allocate()
	}

	singularityNode := *photon.FromBytes[SingularityNode](config.Allocator.Node(0))
	if config.SnapshotID > 0 &&
		(singularityNode.SnapshotRoot.State == stateFree || singularityNode.LastSnapshotID < config.SnapshotID-1) {
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
		if singularityNode.LastSnapshotID == snapshotID-1 {
			snapshotID = singularityNode.LastSnapshotID
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
		config:                      config,
		snapshots:                   snapshots,
		spaces:                      spaces,
		deallocator:                 deallocator,
		pointerNodeAllocator:        pointerNodeAllocator,
		snapshotToNodeNodeAllocator: snapshotToNodeNodeAllocator,
		singularityNode:             singularityNode,
		spacesToCommit:              map[SpaceID]spaceToCommit{},
	}
	return s, nil
}

// Snapshot represents the state at particular point in time.
type Snapshot struct {
	config                      SnapshotConfig
	singularityNode             SingularityNode
	snapshots                   *Space[SnapshotID, SnapshotInfo]
	spaces                      *Space[SpaceID, SpaceInfo]
	deallocator                 Deallocator
	pointerNodeAllocator        SpaceNodeAllocator[NodeAddress]
	snapshotToNodeNodeAllocator SpaceNodeAllocator[DataItem[SnapshotID, NodeAddress]]

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

func (s Snapshot) DeleteSnapshot(snapshotID SnapshotID) error {
	// FIXME (wojciech): Deallocation of last snapshot

	snapshotInfo, exists := s.snapshots.Get(snapshotID)
	if !exists {
		return errors.Errorf("snapshot %d does not exist", snapshotID)
	}
	if snapshotInfo.NextSnapshotID <= s.singularityNode.LastSnapshotID {
		nextSnapshotInfo, exists := s.snapshots.Get(snapshotInfo.NextSnapshotID)
		if !exists {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}

		deallocationLists := NewSpace[SnapshotID, NodeAddress](SpaceConfig[SnapshotID, NodeAddress]{
			SpaceRoot: ParentInfo{
				State: lo.ToPtr(snapshotInfo.DeallocationRoot.State),
				Item:  lo.ToPtr(snapshotInfo.DeallocationRoot.Node),
			},
		})

		nextDeallocationLists := NewSpace[SnapshotID, NodeAddress](SpaceConfig[SnapshotID, NodeAddress]{
			SnapshotID: snapshotInfo.NextSnapshotID,
			HashMod:    &nextSnapshotInfo.DeallocationRoot.HashMod,
			SpaceRoot: ParentInfo{
				State: lo.ToPtr(nextSnapshotInfo.DeallocationRoot.State),
				Item:  lo.ToPtr(nextSnapshotInfo.DeallocationRoot.Node),
			},
			PointerNodeAllocator: s.pointerNodeAllocator,
			DataNodeAllocator:    s.snapshotToNodeNodeAllocator,
			Deallocator:          s.deallocator,
		})

		var startSnapshotID SnapshotID
		if snapshotID == s.singularityNode.FirstSnapshotID {
			startSnapshotID = snapshotID
		} else {
			startSnapshotID = snapshotInfo.PreviousSnapshotID + 1
		}

		for sID := startSnapshotID; sID <= snapshotID; sID++ {
			listNodeAddress, exists := nextDeallocationLists.Get(sID)
			if !exists {
				continue
			}

			list := NewList(ListConfig{
				Item: listNodeAddress,
			})
			list.Deallocate(s.config.Allocator)
			nextDeallocationLists.Delete(sID)
		}

		// FIXME (wojciech): Iterate over space instead
		for sID := s.singularityNode.FirstSnapshotID; sID < snapshotID; sID++ {
			listNodeAddress, exists := deallocationLists.Get(sID)
			if !exists {
				continue
			}

			nextListNodeAddress, _ := nextDeallocationLists.Get(sID)
			nextList := NewList(ListConfig{
				Item: nextListNodeAddress,
			})
			nextList.Attach(listNodeAddress)
			if nextList.config.Item != nextListNodeAddress {
				nextDeallocationLists.Set(sID, nextList.config.Item)
			}
		}

		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
		nextSnapshotInfo.DeallocationRoot = SpaceInfo{
			State:   *nextDeallocationLists.config.SpaceRoot.State,
			Node:    *nextDeallocationLists.config.SpaceRoot.Item,
			HashMod: *nextDeallocationLists.config.HashMod,
		}
		s.snapshots.Set(snapshotInfo.NextSnapshotID, nextSnapshotInfo)
	}

	if snapshotInfo.PreviousSnapshotID > s.singularityNode.FirstSnapshotID {
		previousSnapshotInfo, exists := s.snapshots.Get(snapshotInfo.PreviousSnapshotID)
		if !exists {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID
		s.snapshots.Set(snapshotInfo.PreviousSnapshotID, previousSnapshotInfo)
	}

	if snapshotID == s.singularityNode.FirstSnapshotID {
		s.singularityNode.FirstSnapshotID = snapshotInfo.NextSnapshotID
	}

	// FIXME (wojciech): Deallocate nodes used by deleted snapshot (DeallocationRoot).

	return nil
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
		PreviousSnapshotID: s.singularityNode.LastSnapshotID,
		NextSnapshotID:     s.config.SnapshotID + 1,
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
		FirstSnapshotID: s.singularityNode.FirstSnapshotID,
		LastSnapshotID:  s.config.SnapshotID,
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

	dataNodeAllocator, err := NewSpaceNodeAllocator[DataItem[K, V]](snapshot.config.Allocator)
	if err != nil {
		return nil, err
	}

	return NewSpace[K, V](SpaceConfig[K, V]{
		SnapshotID:           snapshot.config.SnapshotID,
		HashMod:              space.HashMod,
		SpaceRoot:            space.PInfo,
		PointerNodeAllocator: snapshot.pointerNodeAllocator,
		DataNodeAllocator:    dataNodeAllocator,
		Deallocator:          snapshot.deallocator,
	}), nil
}
