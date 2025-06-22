package store

import (
	"fmt"
	"log"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"redis-clone/persistance"
)

type RedisValue interface{}

type MemoryStore struct {
	mu         sync.RWMutex
	data       map[string]interface{}
	expiration map[string]int64
	aof        *persistance.AOF
}

func NewMemoryStoreWithAOF(aof *persistance.AOF) *MemoryStore {
	store := &MemoryStore{
		data:       make(map[string]interface{}),
		expiration: make(map[string]int64),
		aof:        aof,
	}

	go store.expiryDeamon()
	return store
}

func (s *MemoryStore) Set(key string, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = val

	if s.aof != nil {
		s.aof.AppendCommand("SET", key, val)
	}
}

func (s *MemoryStore) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[key]
	if !ok {
		return "", false
	}
	return val.(string), true
}

func (s *MemoryStore) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0

	for _, key := range keys {
		if _, ok := s.data[key]; ok {
			delete(s.data, key)
			delete(s.expiration, key)
			count++

			if s.aof != nil && count > 0 {
				s.aof.AppendCommand("DEL", key)
			}
		}
	}

	return count
}

func (s *MemoryStore) Exists(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0

	for _, key := range keys {
		if _, ok := s.data[key]; ok {
			count++
		}
	}

	return count
}

func (s *MemoryStore) Incr(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, exists := s.data[key]
	if exists {
		strVal, ok := val.(string)
		if !ok {
			return 0, fmt.Errorf("wrong type")
		}
		n, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer")
		}
		n++
		s.data[key] = strconv.FormatInt(n, 10)
		if s.aof != nil {
			s.aof.AppendCommand("INCR", key)
		}

		return n, nil
	}

	// If not exists set to 1
	s.data[key] = "1"
	if s.aof != nil {
		s.aof.AppendCommand("INCR", key)
	}

	return 1, nil
}

func (s *MemoryStore) Type(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, exists := s.data[key]
	if !exists {
		return "none"
	}

	switch val.(type) {
	case string:
		return "string"
	case []string:
		return "list"
	case map[string]string:
		return "hash"
	case map[string]bool:
		return "set"
	default:
		return "unknown"
	}
}

func (s *MemoryStore) Keys(pattern string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0)
	for k := range s.data {
		match, err := path.Match(pattern, k)
		if err != nil {
			continue
		}
		if match {
			keys = append(keys, k)
		}
	}

	return keys
}

func (s *MemoryStore) FlushAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]interface{})
	s.expiration = make(map[string]int64)

	if s.aof != nil {
		s.aof.AppendCommand("FLUSHALL")
	}
}

func (s *MemoryStore) Rename(oldKey, newKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[oldKey]
	if !ok {
		return fmt.Errorf("no such key")
	}
	s.data[newKey] = val
	delete(s.data, oldKey)

	if s.aof != nil {
		s.aof.AppendCommand("RENAME", oldKey, newKey)
	}

	return nil
}

func (s *MemoryStore) Move(key string, db int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return fmt.Errorf("key not found")
	}

	if db != 0 {
		return fmt.Errorf("only one DB implemented")
	}

	return nil
}

func (s *MemoryStore) SaveSnapshot(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return persistance.SaveRDB(path, persistance.Snapshot{
		Data:       s.data,
		Expiration: s.expiration,
	})
}

func (s *MemoryStore) LoadSnapshot(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, err := persistance.LoadRDB(path)
	if err != nil {
		return err
	}
	s.data = snap.Data
	s.expiration = snap.Expiration
	return nil
}

func (s *MemoryStore) ExecuteRaw(cmd string, args []string) string {
	switch strings.ToUpper(cmd) {
	case "PING":
		return "+PONG\r\n"

	case "SET":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for 'set'\r\n"
		}
		s.Set(args[0], args[1])
		return "+OK\r\n"

	case "GET":
		val, ok := s.Get(args[0])
		if !ok {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)

	case "DEL":
		if len(args) < 1 {
			return "-ERR wrong number of arguments for 'del'\r\n"
		}
		count := s.Del(args...)
		return fmt.Sprintf(":%d\r\n", count)

	case "EXISTS":
		count := s.Exists(args...)
		return fmt.Sprintf(":%d\r\n", count)

	case "LPUSH":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for 'lpush'\r\n"
		}
		count := s.LPush(args[0], args[1:]...)
		return fmt.Sprintf(":%d\r\n", count)

	case "RPUSH":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for 'rpush'\r\n"
		}
		count := s.RPush(args[0], args[1:]...)
		return fmt.Sprintf(":%d\r\n", count)

	case "LPOP":
		if len(args) < 1 {
			return "-ERR wrong number of arguments for 'lpop'\r\n"
		}
		val, err := s.LPop(args[0])
		if err != nil {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)

	case "RPOP":
		if len(args) < 1 {
			return "-ERR wrong number of arguments for 'rpop'\r\n"
		}
		val, err := s.RPop(args[0])
		if err != nil {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)

	case "LRANGE":
		if len(args) != 3 {
			return "-ERR wrong number of arguments for 'lrange'\r\n"
		}
		start, err1 := strconv.Atoi(args[1])
		stop, err2 := strconv.Atoi(args[2])
		if err1 != nil || err2 != nil {
			return "-ERR start and stop must be integers\r\n"
		}

		items, err := s.LRange(args[0], start, stop)
		if err != nil {
			return "-ERR" + err.Error() + "\r\n"
		}

		resp := fmt.Sprintf("*%d\r\n", len(items))
		for _, item := range items {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(item), item)
		}
		return resp

	case "SADD":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for 'sadd'\r\n"
		}

		count := s.SAdd(args[0], args[1:]...)
		return fmt.Sprintf(":%d\r\n", count)

	case "SREM":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for 'srem'\r\n"
		}

		count := s.SRem(args[0], args[1:]...)
		return fmt.Sprintf(":%d\r\n", count)

	case "SISMEMBER":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'sismember'\r\n"
		}
		if s.SIsMember(args[0], args[1]) {
			return ":1\r\n"
		}
		return ":0\r\n"

	case "SMEMBERS":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'smembers'\r\n"
		}
		members, ok := s.SMembers(args[0])
		if !ok {
			return "*0\r\n"
		}
		resp := fmt.Sprintf("*%d\r\n", len(members))
		for _, m := range members {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(m), m)
		}
		return resp

	case "SCARD":
		if len(args) != 1 {
			return "-ERR wrong number of arguemnts for 'scard'\r\n"
		}
		count := s.SCard(args[0])
		return fmt.Sprintf(":%d\r\n", count)

	case "SUNION":
		if len(args) < 1 {
			return "-ERR wrong number of arguments for 'sunion'\r\n"
		}
		union := s.SUnion(args...)
		resp := fmt.Sprintf("*%d\r\n", len(union))
		for _, m := range union {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(m), m)
		}
		return resp

	case "HSET":
		if len(args) != 3 {
			return "-ERR wrong number of arguments for 'hset'\r\n"
		}
		added := s.HSet(args[0], args[1], args[2])
		return fmt.Sprintf(":%d\r\n", added)

	case "HGET":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'hget'\r\n"
		}
		val, ok := s.HGet(args[0], args[1])
		if !ok {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)

	case "HGETALL":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'hgetall'\r\n"
		}
		pairs, ok := s.HGetAll(args[0])
		if !ok {
			return "*0\r\n"
		}
		resp := fmt.Sprintf("*%d\r\n", len(pairs))
		for _, v := range pairs {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
		}
		return resp

	case "HDEL":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for 'hdel'\r\n"
		}
		count := s.HDel(args[0], args[1:]...)
		return fmt.Sprintf(":%d\r\n", count)

	case "HEXISTS":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'hexists'\r\n"
		}
		exists := s.HExists(args[0], args[1])
		if exists {
			return ":1\r\n"
		}
		return ":0\r\n"

	case "EXPIRE":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'expire'\r\n"
		}
		seconds, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || seconds < 0 {
			return "-ERR invalid expire time\r\n"
		}
		ok := s.Expire(args[0], seconds)
		if ok {
			return ":1\r\n"
		}
		return ":0\r\n"

	case "TTL":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'ttl'\r\n"
		}
		ttl := s.TTL(args[0])
		return fmt.Sprintf(":%d\r\n", ttl)

	case "SAVE":
		err := s.LoadSnapshot("dump.rdb")
		if err != nil {
			log.Println(err)
			return "-ERR failed to save snapshot\r\n"
		}
		return "+OK\r\n"

	case "INCR":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'incr'\r\n"
		}
		n, err := s.Incr(args[0])
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
		return fmt.Sprintf(":%d\r\n", n)

	case "HINCRBY":
		if len(args) != 3 {
			return "-ERR wrong number of arguments for 'hincrby'\r\n"
		}
		incr, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return "-ERR increment must be integer\r\n"
		}
		n, err := s.HIncrBy(args[0], args[1], incr)
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
		return fmt.Sprintf(":%d\r\n", n)

	case "TYPE":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'type'\r\n"
		}
		t := s.Type(args[0])
		return fmt.Sprintf("+%s\r\n", t)

	case "KEYS":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'keys'\r\n"
		}
		keys := s.Keys(args[0])
		resp := fmt.Sprintf("*%d\r\n", len(keys))
		for _, k := range keys {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
		}
		return resp

	case "FLUSHALL":
		s.FlushAll()
		return "+OK\r\n"

	case "RENAME":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'rename'\r\n"
		}
		err := s.Rename(args[0], args[1])
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
		return "+OK\r\n"

	case "MOVE":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'move'\r\n"
		}
		dbIndex, err := strconv.Atoi(args[1])
		if err != nil {
			return "-ERR invalid DB index\r\n"
		}
		err = s.Move(args[0], dbIndex)
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
		return ":1\r\n"

	default:
		return "-ERR unknown command\r\n"
	}
}

func (s *MemoryStore) StartAutoSave(path string, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		for range ticker.C {
			if err := s.SaveSnapshot(path); err == nil {
				log.Println("Snapshot saved.")
			} else {
				log.Println("Failed to save snapshot:", err)
			}
		}

	}()
}

func (s *MemoryStore) SetAOF(aof *persistance.AOF) {
	s.aof = aof
}
