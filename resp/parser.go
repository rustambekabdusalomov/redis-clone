package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Parse minimal RESP input, only handles simple commands
func Parse(reader *bufio.Reader) (string, []string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", nil, err
	}

	line = strings.Trim(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return "", nil, errors.New("invalid RESP")
	}

	// Read actual args
	args := make([]string, 0)
	count := 0
	fmt.Sscanf(line, "*%d", &count)
	for i := 0; i < count; i++ {
		reader.ReadString('\n') // Skip length line
		arg, _ := reader.ReadString('\n')
		args = append(args, strings.Trim(arg, "\r\n"))
	}
	if len(args) == 0 {
		return "", nil, errors.New("empty command")
	}

	return args[0], args[1:], nil
}

func ParseRESP(r *bufio.Reader) (string, []string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", nil, err
	}
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "*") {
		return "", nil, errors.New("invalid RESP array")
	}
	numArgs, err := strconv.Atoi(line[1:])
	if err != nil {
		return "", nil, errors.New("invalid array size")
	}

	parts := make([]string, 0, numArgs)
	for i := 0; i < numArgs; i++ {
		lenLine, err := r.ReadString('\n')
		if err != nil {
			return "", nil, err
		}
		if !strings.HasPrefix(lenLine, "$") {
			return "", nil, errors.New("expected bulk string")
		}
		strLen, err := strconv.Atoi(strings.TrimSpace(lenLine[1:]))
		if err != nil {
			return "", nil, errors.New("invalid bulk length")
		}

		buf := make([]byte, strLen+2) // \r\n
		_, err = io.ReadFull(r, buf)
		if err != nil {
			return "", nil, err
		}
		str := string(buf[:strLen])
		parts = append(parts, str)
	}

	if len(parts) == 0 {
		return "", nil, errors.New("empty command")
	}
	return parts[0], parts[1:], nil
}
