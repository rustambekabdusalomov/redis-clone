package persistance

import (
	"encoding/gob"
	"os"
)

type Snapshot struct {
	Data       map[string]any
	Expiration map[string]int64
}

func SaveRDB(file string, snapshot Snapshot) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := gob.NewEncoder(f)
	return encoder.Encode(snapshot)
}

func LoadRDB(file string) (Snapshot, error) {
	snap := Snapshot{}
	f, err := os.Open(file)
	if err != nil {
		return Snapshot{}, err
	}
	defer f.Close()

	decoder := gob.NewDecoder(f)
	err = decoder.Decode(&snap)
	return snap, err
}
