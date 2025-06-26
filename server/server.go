package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	"redis-clone/resp"
	"redis-clone/store"
)

type Server struct {
	addr  string
	store *store.MemoryStore
}

func New(addr string) *Server {
	return &Server{
		addr: addr,
	}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	subs := make(map[string]chan string)

	for {
		cmd, args, err := resp.Parse(reader)
		if err != nil {
			conn.Write([]byte("-ERR invalid command\r\n"))
			continue
		}

		resp := s.executeCommand(cmd, args, conn, subs)
		conn.Write([]byte(resp))
	}

	for chName, subCh := range subs {
		s.store.Unsubscribe(chName, subCh)
		close(subCh)
	}
}

func (s *Server) executeCommand(cmd string, args []string, conn net.Conn, subs map[string]chan string) string {
	switch strings.ToUpper(cmd) {
	case "PING":
		return "+PONG\r\n"

	case "SET":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for 'set'\r\n"
		}
		s.store.Set(args[0], args[1])
		return "+OK\r\n"

	case "GET":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'get'\r\n"
		}
		val, ok := s.store.Get(args[0])
		if !ok {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)

	case "DEL":
		if len(args) < 1 {
			return "-ERR wrong number of arguments for 'del'\r\n"
		}

		count := s.store.Del(args...)
		return fmt.Sprintf(":%d\r\n", count)

	case "EXISTS":
		count := s.store.Exists(args...)
		return fmt.Sprintf(":%d\r\n", count)

	case "LPUSH":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for 'lpush'\r\n"
		}
		count := s.store.LPush(args[0], args[1:]...)
		return fmt.Sprintf(":%d\r\n", count)

	case "RPUSH":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for 'rpush'\r\n"
		}
		count := s.store.RPush(args[0], args[1:]...)
		return fmt.Sprintf(":%d\r\n", count)

	case "LPOP":
		if len(args) < 1 {
			return "-ERR wrong number of arguments for 'lpop'\r\n"
		}
		val, err := s.store.LPop(args[0])
		if err != nil {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)

	case "RPOP":
		if len(args) < 1 {
			return "-ERR wrong number of arguments for 'rpop'\r\n"
		}
		val, err := s.store.RPop(args[0])
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

		items, err := s.store.LRange(args[0], start, stop)
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

		count := s.store.SAdd(args[0], args[1:]...)
		return fmt.Sprintf(":%d\r\n", count)

	case "SREM":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for 'srem'\r\n"
		}

		count := s.store.SRem(args[0], args[1:]...)
		return fmt.Sprintf(":%d\r\n", count)

	case "SISMEMBER":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'sismember'\r\n"
		}
		if s.store.SIsMember(args[0], args[1]) {
			return ":1\r\n"
		}
		return ":0\r\n"

	case "SMEMBERS":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'smembers'\r\n"
		}
		members, ok := s.store.SMembers(args[0])
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
		count := s.store.SCard(args[0])
		return fmt.Sprintf(":%d\r\n", count)

	case "SUNION":
		if len(args) < 1 {
			return "-ERR wrong number of arguments for 'sunion'\r\n"
		}
		union := s.store.SUnion(args...)
		resp := fmt.Sprintf("*%d\r\n", len(union))
		for _, m := range union {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(m), m)
		}
		return resp

	case "HSET":
		if len(args) != 3 {
			return "-ERR wrong number of arguments for 'hset'\r\n"
		}
		added := s.store.HSet(args[0], args[1], args[2])
		return fmt.Sprintf(":%d\r\n", added)

	case "HGET":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'hget'\r\n"
		}
		val, ok := s.store.HGet(args[0], args[1])
		if !ok {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)

	case "HGETALL":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'hgetall'\r\n"
		}
		pairs, ok := s.store.HGetAll(args[0])
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
		count := s.store.HDel(args[0], args[1:]...)
		return fmt.Sprintf(":%d\r\n", count)

	case "HEXISTS":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'hexists'\r\n"
		}
		exists := s.store.HExists(args[0], args[1])
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
		ok := s.store.Expire(args[0], seconds)
		if ok {
			return ":1\r\n"
		}
		return ":0\r\n"

	case "TTL":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'ttl'\r\n"
		}
		ttl := s.store.TTL(args[0])
		return fmt.Sprintf(":%d\r\n", ttl)

	case "SAVE":
		err := s.store.LoadSnapshot("dump.rdb")
		if err != nil {
			log.Println(err)
			return "-ERR failed to save snapshot\r\n"
		}
		return "+OK\r\n"

	case "INCR":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'incr'\r\n"
		}
		n, err := s.store.Incr(args[0])
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
		n, err := s.store.HIncrBy(args[0], args[1], incr)
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
		return fmt.Sprintf(":%d\r\n", n)

	case "TYPE":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'type'\r\n"
		}
		t := s.store.Type(args[0])
		return fmt.Sprintf("+%s\r\n", t)

	case "KEYS":
		if len(args) != 1 {
			return "-ERR wrong number of arguments for 'keys'\r\n"
		}
		keys := s.store.Keys(args[0])
		resp := fmt.Sprintf("*%d\r\n", len(keys))
		for _, k := range keys {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
		}
		return resp

	case "FLUSHALL":
		s.store.FlushAll()
		return "+OK\r\n"

	case "RENAME":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'rename'\r\n"
		}
		err := s.store.Rename(args[0], args[1])
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
		err = s.store.Move(args[0], dbIndex)
		if err != nil {
			return "-ERR " + err.Error() + "\r\n"
		}
		return ":1\r\n"

	case "SUBSCRIBE":
		if len(args) != 1 {
			return "-ERR SUBSCRIBE requires a channel\r\n"
		}

		ch := make(chan string, 100)
		s.store.Subscribe(args[0], ch)
		// conn.Write([]byte(fmt.Sprintf("$9\r\nsubscribed\r\n$%d\r\n%s\r\n", len(args[0]), args[0])))

		go func() {
			for msg := range ch {
				reply := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
					len(args[0]), args[0], len(msg), msg)
				conn.Write([]byte(reply))
			}
		}()
		return "+OK\r\n" // no immediate reply, subscription is async

	case "PUBLISH":
		if len(args) != 2 {
			return "-ERR PUBLISH requires channel and message\r\n"
		}
		count := s.store.Publish(args[0], args[1])
		return fmt.Sprintf(":%d\r\n", count)

	case "UNSUBSCRIBE":
		if len(args) < 1 {
			return "-ERR UNSUBSCRIBE requires at least one channel\r\n"
		}

		for _, chName := range args {
			subCh, ok := subs[chName]
			if ok {
				s.store.Unsubscribe(chName, subCh)
				close(subCh)
				delete(subs, chName)

				conn.Write([]byte(fmt.Sprintf("*2\r\n$11\r\nunsubscribed\r\n$%d\r\n%s\r\n", len(chName), chName)))
			} else {
				conn.Write([]byte(fmt.Sprintf("*2\r\n$11\r\nunsubscribed\r\n$%d\r\n%s\r\n", len(chName), chName)))
			}
		}
		return ""

	default:
		return "-ERR unknown command\r\n"
	}
}

func (s *Server) Load(path string) error {
	return s.store.LoadSnapshot(path)
}

func (s *Server) AttachStore(store *store.MemoryStore) {
	s.store = store
}
