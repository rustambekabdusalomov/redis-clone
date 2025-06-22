package main

import (
	"log"
	"time"

	"redis-clone/persistance"
	"redis-clone/server"
	"redis-clone/store"
)

func main() {
	// === Load AOF (Append Only File) ===
	aof, err := persistance.NewAOF("appendonly.aof")
	if err != nil {
		log.Fatalln(err)
	}

	// === Initialize in-memory store with AOF support ===
	memStore := store.NewMemoryStoreWithAOF(nil)

	// === Load RDB snapshot if available ===
	if err = memStore.LoadSnapshot("dump.rdb"); err == nil {
		log.Println("[RDB] Snapshot loaded from dump.rdb")
	} else {
		log.Println("[RDB] No snapshot found")
	}

	// Replay AOF commands
	err = aof.Replay("appendonly.aof", func(cmd string, args []string) error {
		memStore.ExecuteRaw(cmd, args) // You need to implement this
		return nil
	})
	if err != nil {
		log.Println("[AOF] Replay error:", err)
	} else {
		log.Println("[AOF] Replay completed")
	}

	// == Set AOF for memStore
	memStore.SetAOF(aof)

	// === Start auto-saving RDB every 10 seconds ===
	memStore.StartAutoSave("dump.rdb", 10*time.Second)

	s := server.New(":6399")
	s.AttachStore(memStore)

	log.Println("Server running on port: 6399...")
	if err := s.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
