package store

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type PartitionManager struct {
	partitions []*MemoryStore
	num        int
}

func NewPartitionManager(num int) *PartitionManager {
	partitions := make([]*MemoryStore, num)
	for i := 0; i < num; i++ {
		partitions[i] = NewMemoryStoreWithAOF(nil)
	}
	return &PartitionManager{
		partitions: partitions,
		num:        num,
	}
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (pm *PartitionManager) getParition(key string) *MemoryStore {
	index := hashKey(key) % uint32(pm.num)
	return pm.partitions[index]
}

func (pm *PartitionManager) Set(key, value string) {
	pm.getParition(key).Set(key, value)
}

func (pm *PartitionManager) Get(key string) (string, bool) {
	return pm.getParition(key).Get(key)
}

func (pm *PartitionManager) Del(keys ...string) int {
	count := 0
	for _, k := range keys {
		count += pm.getParition(k).Del(k)
	}
	return count
}

func (pm *PartitionManager) Exists(keys ...string) int {
	count := 0
	for _, k := range keys {
		count += pm.getParition(k).Exists(k)
	}
	return count
}

func (pm *PartitionManager) Incr(key string) (int64, error) {
	return pm.getParition(key).Incr(key)
}

func (pm *PartitionManager) Type(key string) string {
	return pm.getParition(key).Type(key)
}

func (pm *PartitionManager) Keys(pattern string) []string {
	wg := sync.WaitGroup{}

	resultsCh := make(chan []string, pm.num)

	for _, p := range pm.partitions {
		wg.Add(1)
		go func(part *MemoryStore) {
			defer wg.Done()
			resultsCh <- part.Keys(pattern)
		}(p)
	}

	wg.Wait()
	close(resultsCh)

	allKeys := make([]string, 0)
	for keys := range resultsCh {
		allKeys = append(allKeys, keys...)
	}

	return allKeys
}

func (pm *PartitionManager) FlushAll() {
	for _, p := range pm.partitions {
		p.FlushAll()
	}
}

func (pm *PartitionManager) Rename(oldKey, newKey string) error {
	oldParitition := pm.getParition(oldKey)
	newPartition := pm.getParition(newKey)

	if oldParitition == newPartition {
		return oldParitition.Rename(oldKey, newKey)
	}

	// Cross partition re-name
	oldParitition.mu.RLock()
	val, ok := oldParitition.data[oldKey]
	oldParitition.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no such key")
	}

	newPartition.mu.Lock()
	newPartition.data[newKey] = val
	newPartition.mu.Unlock()

	oldParitition.mu.Lock()
	delete(oldParitition.data, oldKey)
	oldParitition.mu.Unlock()

	// AOF logging
	if oldParitition.aof != nil {
		oldParitition.aof.AppendCommand("RENAME", oldKey, newKey)
	}
	if newPartition != oldParitition && newPartition.aof != nil {
		newPartition.aof.AppendCommand("RENAME", oldKey, newKey)
	}

	return nil
}

func (pm *PartitionManager) Move(key string, db int) error {
	return pm.getParition(key).Move(key, db)
}
