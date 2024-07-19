package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

type KeyValueStore struct {
	store map[string]string
	mutex sync.RWMutex
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		store: make(map[string]string),
	}
}

func (kvs *KeyValueStore) Set(key, value string) {
	kvs.mutex.Lock()
	defer kvs.mutex.Unlock()
	kvs.store[key] = value
}

func (kvs *KeyValueStore) Get(key string) (string, bool) {
	kvs.mutex.RLock()
	defer kvs.mutex.RUnlock()
	value, exists := kvs.store[key]
	return value, exists
}

func (kvs *KeyValueStore) Delete(key string) {
	kvs.mutex.Lock()
	defer kvs.mutex.Unlock()
	delete(kvs.store, key)
}

func handleConnection(conn net.Conn, kvs *KeyValueStore) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		input := scanner.Text()
		args := strings.Fields(input)
		if len(args) < 2 {
			conn.Write([]byte("ERR invalid command\n"))
			log.Printf("Invalid command: %s\n", input)
			continue
		}

		switch args[0] {
		case "GET":
			value, exists := kvs.Get(args[1])
			if !exists {
				conn.Write([]byte("ERR key not found\n"))
				fmt.Println("Key not found:", args[1])
				continue
			}
			conn.Write([]byte(value))
			log.Printf("GET %s: %s\n", args[1], value)
			continue
		case "SET":
			kvs.Set(args[1], args[2])
			conn.Write([]byte("OK\n"))
			log.Printf("SET %s: %s\n", args[1], args[2])
			continue
		case "DEL":
			kvs.Delete(args[1])
			conn.Write([]byte("OK\n"))
			continue
		default:
			conn.Write([]byte("ERR invalid command\n"))
		}
	}
}

func main() {
	kvs := NewKeyValueStore()
	listener, err := net.Listen("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("Server is running on localhost:6379")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn, kvs)
	}
}
