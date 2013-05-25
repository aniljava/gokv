/**
 * Very simple KV Store in go backed by file system
 */
package gokv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

const KEY_POINTER_SIZE int = 8 //int64

type DBConfig struct {
	NUMBER_OF_BUCKETS uint32
	DBPath            string
}

type DB struct {
	db_config      DBConfig
	keys           []byte //Inmemory copy of keys.
	file           os.File
	num_of_buckets int64
}

func DefaultConfig() DBConfig {
	config := DBConfig{}

	config.NUMBER_OF_BUCKETS = 999983
	config.DBPath = "default.db"

	return config
}

/**
 *
 */
func Open(config DBConfig) (*DB, error) {

	if _, err := os.Stat(config.DBPath); os.IsNotExist(err) {
		fmt.Print("Does not exist ")
		if err = Create(config); err != nil {
			return nil, err
		}
		return Open(config)
	}

	file, err := os.Create(config.DBPath)
	if err != nil {
		return nil, err
	}

	db := DB{}
	readbuffer := make([]byte, 8)
	_, err = file.Read(readbuffer)

	if err != nil {
		return nil, err
	}
	db.num_of_buckets = bytes_to_int64(readbuffer)
	//Size and other metadata

	db.keys = make([]byte, config.NUMBER_OF_BUCKETS*KEY_POINTER_SIZE)
	_, err = file.Read(db.keys)

	//TODO
	// IF FILE EXISTS READ METADATA

	// READ METADATA :
	// Copy Keyspace
	return &db, nil
}

func Create(config DBConfig) error {

	file, err := os.Create(config.DBPath)
	defer file.Close()

	if err != nil {
		return err
	}

	//First 512 bytes are reserved for metadata
	// Change Open while making changes here
	file.Write(int32_to_bytes(config.NUMBER_OF_BUCKETS)) //NUM OF BUCKETS
	file.Write(int64_to_bytes(0))                        //TOTAL DB SIZE
	file.Write(int64_to_bytes(0))                        //NUM OF ELEMENTS

	filler := make([]byte, (512 - 24))
	file.Write(filler)

	nullbytes := make([]byte, config.NUMBER_OF_BUCKETS*KEY_POINTER_SIZE)
	_, err = file.Write(nullbytes)
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) close() error {
	//TODO
	//ACTIVATE READONLY
	//WAIT FOR WRITE JOB TO FINISH.
	//CLOSE
	return nil
}

func (db *DB) get(key []byte) ([]byte, error) {

	hash := Hash32(key)
	index := uint32(hash) % db.db_config.NUMBER_OF_BUCKETS
	fmt.Print(hash)

	//TODO
	// Get blockinfo from keyspace.
	// Get block data either from hash or file
	// If okay return value or return error
	return nil, nil
}

func (db *DB) set(key []byte, value []byte) error {
	//TODO
	//
	return nil
}

func (db *DB) remove(key []byte) (bool, error) {
	//TODO
	return false, nil
}

func (db *DB) exists(key []byte) (bool, error) {
	//TODO
	return false, nil
}

func (db *DB) sync() error {
	//TODO
	return nil
}

/** UTILS **/
func int64_to_bytes(a int64) []byte {
	ret := make([]byte, 8)

	ret[0] = (byte)(a & 0xFF)
	ret[1] = (byte)((a >> 8) & 0xFF)
	ret[2] = (byte)((a >> 16) & 0xFF)
	ret[3] = (byte)((a >> 24) & 0xFF)
	ret[4] = (byte)((a >> 32) & 0xFF)
	ret[5] = (byte)((a >> 40) & 0xFF)
	ret[6] = (byte)((a >> 48) & 0xFF)
	ret[7] = (byte)((a >> 56) & 0xFF)
	return ret
}

func int32_to_bytes(a int) []byte {
	ret := make([]byte, 8)

	ret[0] = (byte)(a & 0xFF)
	ret[1] = (byte)((a >> 8) & 0xFF)
	ret[2] = (byte)((a >> 16) & 0xFF)
	ret[3] = (byte)((a >> 24) & 0xFF)
	return ret
}

func bytes_to_int32(b []byte) uint32 {
	return uint32(b[0]) + uint32(b[1])<<8 + uint32(b[2])<<16 + uint32(b[3])<<24
}

func bytes_to_int64(b []byte) int64 {
	return int64(b[0]) + int64(b[1])<<8 + int64(b[2])<<16 + int64(b[3])<<24 + int64(b[4])<<32 + int64(b[5])<<40 + int64(b[6])<<48 + int64(b[7])<<56
}

//MMH3 from https://github.com/reusee/mmh3/blob/master/mmh3.go
func Hash32(key []byte) uint32 {
	length := len(key)
	if length == 0 {
		return 0
	}
	var c1, c2 uint32 = 0xcc9e2d51, 0x1b873593
	nblocks := length / 4
	var h, k uint32
	buf := bytes.NewBuffer(key)
	for i := 0; i < nblocks; i++ {
		binary.Read(buf, binary.LittleEndian, &k)
		k *= c1
		k = (k << 15) | (k >> (32 - 15))
		k *= c2
		h ^= k
		h = (h << 13) | (h >> (32 - 13))
		h = (h * 5) + 0xe6546b64
	}
	k = 0
	tailIndex := nblocks * 4
	switch length & 3 {
	case 3:
		k ^= uint32(key[tailIndex+2]) << 16
		fallthrough
	case 2:
		k ^= uint32(key[tailIndex+1]) << 8
		fallthrough
	case 1:
		k ^= uint32(key[tailIndex])
		k *= c1
		k = (k << 13) | (k >> (32 - 15))
		k *= c2
		h ^= k
	}
	h ^= uint32(length)
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}
