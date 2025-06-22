package store

import "errors"

func (s *MemoryStore) getList(key string) ([]string, bool) {
	val, ok := s.data[key]
	if !ok {
		return nil, false
	}

	list, ok := val.([]string)
	return list, ok
}

func (s *MemoryStore) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, _ := s.getList(key)
	list = append(values, list...)

	s.data[key] = list

	if s.aof != nil && len(values) > 0 {
		_ = s.aof.AppendCommand("LPUSH", append([]string{key}, values...)...)
	}

	return len(list)
}

func (s *MemoryStore) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, _ := s.getList(key)
	list = append(list, values...)
	s.data[key] = list

	if s.aof != nil && len(values) > 0 {
		_ = s.aof.AppendCommand("RPUSH", append([]string{key}, values...)...)
	}

	return len(list)
}

func (s *MemoryStore) LPop(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, ok := s.getList(key)
	if !ok {
		return "", errors.New("empty or not a list")
	}

	val := list[0]
	list = list[1:]
	s.data[key] = list

	// AOF logging
	if s.aof != nil {
		_ = s.aof.AppendCommand("LPOP", key)
	}

	return val, nil
}

func (s *MemoryStore) RPop(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, ok := s.getList(key)
	if !ok {
		return "", errors.New("empty or not a list")
	}

	val := list[len(list)-1]
	list = list[:len(list)-1]
	s.data[key] = list

	// AOF logging
	if s.aof != nil {
		_ = s.aof.AppendCommand("RPOP", key)
	}

	return val, nil
}

func (s *MemoryStore) LRange(key string, start, stop int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, ok := s.getList(key)
	if !ok {
		return nil, errors.New("not a list")
	}

	if start < 0 {
		start = len(list) + start
	}
	if stop < 0 {
		stop = len(list) + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= len(list) {
		stop = len(list) - 1
	}
	if start > stop || start >= len(list) {
		return []string{}, nil
	}

	return list[start : stop+1], nil
}
