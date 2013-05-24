/**
 * Very simple KV Store
 */
package gokv

import (
	"fmt"
	"os"
)

const KEY_POINTER_SIZE int = 8 //int64

type DBConfig struct {
	NUMBER_OF_BUCKETS int
	DBPath            string
}

type DB struct {
	db_config      DBConfig
	keys           []byte //Inmemory copy of keys.
	file           os.File
	num_of_buckets int
}

func DefaultConfig() DBConfig {
	config := DBConfig{}

	config.NUMBER_OF_BUCKETS = 999983
	config.DBPath = "/root/Desktop/default.db"

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

	//TODO
	// IF FILE EXISTS READ METADATA

	// READ METADATA :
	// Copy Keyspace
	return nil, nil
}

func Create(config DBConfig) error {

	file, err := os.Create(config.DBPath)
	defer file.Close()

	if err != nil {
		return err
	}

	//METADATA

	file.Write(int32_to_bytes(config.NUMBER_OF_BUCKETS)) //NUM OF BUCKETS
	file.Write(int64_to_bytes(0))                        //TOTAL DB SIZE
	file.Write(int64_to_bytes(0))                        //NUM OF ELEMENTS
	nullbytes := make([]byte, config.NUMBER_OF_BUCKETS*KEY_POINTER_SIZE)
	total, err := file.Write(nullbytes)
	fmt.Print(total)
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
