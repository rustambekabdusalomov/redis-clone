package store

func (s *MemoryStore) getHash(key string) (map[string]string, bool) {
	val, ok := s.data[key]
	if !ok {
		return nil, false
	}
	hash, ok := val.(map[string]string)
	return hash, ok
}

func (s *MemoryStore) HSet(key, field, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash, _ := s.getHash(key)
	if hash == nil {
		hash = make(map[string]string)
	}
	_, exists := hash[field]
	hash[field] = value
	s.data[key] = hash
	if exists {
		return 0
	}

	if s.aof != nil {
		s.aof.AppendCommand("HSET", key, field, value)
	}

	return 1
}

func (s *MemoryStore) HGet(key, field string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash, ok := s.getHash(key)
	if !ok {
		return "", false
	}
	val, exists := hash[field]
	return val, exists
}

func (s *MemoryStore) HGetAll(key string) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash, ok := s.getHash(key)
	if !ok {
		return nil, false
	}
	result := make([]string, 0, len(hash)*2)
	for field, value := range hash {
		result = append(result, field, value)
	}
	return result, true
}

func (s *MemoryStore) HDel(key string, fields ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash, ok := s.getHash(key)
	if !ok {
		return 0
	}
	count := 0
	for _, field := range fields {
		if _, exists := hash[field]; exists {
			delete(hash, field)
			count++
		}
	}
	s.data[key] = hash

	if s.aof != nil && count > 0 {
		s.aof.AppendCommand("HDEL", append([]string{key}, fields...)...)
	}

	return count
}

func (s *MemoryStore) HLen(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash, ok := s.getHash(key)
	if !ok {
		return 0
	}
	return len(hash)
}

func (s *MemoryStore) HExists(key, field string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash, ok := s.getHash(key)
	if !ok {
		return false
	}

	_, exists := hash[field]
	return exists
}
