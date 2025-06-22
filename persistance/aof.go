package persistance

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"redis-clone/resp"
)

type AOF struct {
	file *os.File
}

func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &AOF{
		file: f,
	}, nil
}

func (a *AOF) AppendCommand(cmd string, args ...string) error {
	// Format as RESP command
	line := fmt.Sprintf("*%d\r\n", len(args)+1)
	line += fmt.Sprintf("$%d\r\n%s\r\n", len(cmd), cmd)
	for _, arg := range args {
		line += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	_, err := a.file.WriteString(line)
	return err
}

func (a *AOF) Close() error {
	return a.file.Close()
}

func (a *AOF) Replay(path string, handle func(cmd string, args []string) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		cmd, args, err := resp.ParseRESP(reader)
		if err != nil {
			break // End of file or invalid
		}
		log.Println(cmd, args)
		if err = handle(cmd, args); err != nil {
			return err
		}
	}

	return nil
}
