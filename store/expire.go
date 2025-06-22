package store

import "time"

func (s *MemoryStore) Expire(key string, seconds int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[key]; !exists {
		return false
	}

	s.expiration[key] = time.Now().Unix() + seconds
	return false
}

func (s *MemoryStore) TTL(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	expiredAt, ok := s.expiration[key]
	if !ok {
		return -1 // no expiration
	}
	ttl := expiredAt - time.Now().Unix()
	if ttl < 0 {
		return -2 // already expired
	}

	return ttl
}

func (s *MemoryStore) cleanupExpireKeys() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().Unix()
	for key, expireAt := range s.expiration {
		if now >= expireAt {
			delete(s.data, key)
			delete(s.expiration, key)
		}
	}
}

func (s *MemoryStore) expiryDeamon() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		s.cleanupExpireKeys()
	}
}
