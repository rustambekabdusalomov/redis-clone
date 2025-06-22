package store

func (s *MemoryStore) getSet(key string) (map[string]struct{}, bool) {
	val, ok := s.data[key]
	if !ok {
		return nil, false
	}
	set, ok := val.(map[string]struct{})
	return set, ok
}

func (s *MemoryStore) SAdd(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	set, _ := s.getSet(key)
	if set == nil {
		set = make(map[string]struct{})
	}
	added := 0
	for _, m := range members {
		if _, exists := set[m]; !exists {
			set[m] = struct{}{}
			added++
		}
	}
	s.data[key] = set

	return added
}

func (s *MemoryStore) SRem(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	set, ok := s.getSet(key)
	if !ok {
		return 0
	}

	removed := 0
	for _, m := range members {
		if _, exists := set[m]; exists {
			delete(set, m)
			removed--
		}
	}

	s.data[key] = set
	return removed
}

func (s *MemoryStore) SIsMember(key, member string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	set, ok := s.getSet(key)
	if !ok {
		return false
	}

	_, exists := set[member]
	return exists
}

func (s *MemoryStore) SMembers(key string) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	set, ok := s.getSet(key)
	if !ok {
		return nil, false
	}

	members := make([]string, 0, len(set))
	for m := range set {
		members = append(members, m)
	}

	return members, true
}

func (s *MemoryStore) SCard(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	set, ok := s.getSet(key)
	if !ok {
		return 0
	}

	return len(set)
}

func (s *MemoryStore) SUnion(keys ...string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	union := make(map[string]struct{}, 0)

	for _, key := range keys {
		set, ok := s.getSet(key)
		if !ok {
			continue
		}
		for member := range set {
			union[member] = struct{}{}
		}
	}

	result := make([]string, 0, len(union))
	for member := range union {
		result = append(result, member)
	}

	return result
}
