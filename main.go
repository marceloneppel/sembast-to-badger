package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/dgraph-io/badger"
)

const invalidSembastFileMessage = "invalid sembast file"

// exists returns whether the given file or directory exists
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func getBytes(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func main() {
	args := os.Args
	var sembastFile = ""
	var badgerDir = ""
	for argIndex, arg := range args {
		if len(args) >= argIndex {
			if arg == "-s" {
				sembastFile = args[argIndex+1]
			}
			if arg == "-b" {
				badgerDir = args[argIndex+1]
			}
		}
	}
	if sembastFile != "" && badgerDir != "" {
		badgerDirExists, err := exists(badgerDir)
		if err != nil {
			log.Fatal(err)
		}
		if badgerDirExists {
			log.Fatal(errors.New("badger directory already exists"))
		}
		// Open our jsonFile
		jsonFile, err := os.Open(sembastFile)
		// if we os.Open returns an error then handle it
		if err != nil {
			log.Fatal(err)
		}
		// defer the closing of our jsonFile so that we can parse it later on
		defer jsonFile.Close()
		byteValue, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			log.Fatal(err)
		}
		var result map[string]interface{}
		err = json.Unmarshal([]byte(byteValue), &result)
		if err != nil {
			log.Fatal(err)
		}
		if stores, ok := result["stores"]; ok {
			//fmt.Println(stores)
			storesSlice := stores.([]interface{})
			// TODO: allow multiple stores.
			if len(storesSlice) == 1 {
				//fmt.Println(storesSlice[0])
				if keys, ok := storesSlice[0].(map[string]interface{})["keys"]; ok {
					//fmt.Println(keys)
					if values, ok := storesSlice[0].(map[string]interface{})["values"]; ok {
						//fmt.Println(values)
						keysSlice := keys.([]interface{})
						valuesSlice := values.([]interface{})
						if len(keysSlice) == len(valuesSlice) {
							opts := badger.DefaultOptions
							opts.Dir = badgerDir
							opts.ValueDir = badgerDir
							db, err := badger.Open(opts)
							if err != nil {
								log.Fatal(err)
							}
							defer db.Close()
							for keyIndex, key := range keysSlice {
								valueBytes, err := getBytes(valuesSlice[keyIndex])
								if err != nil {
									log.Fatal(err)
								}
								err = db.Update(func(txn *badger.Txn) error {
									return txn.Set([]byte(key.(string)), valueBytes)
								})
								if err != nil {
									log.Fatal(err)
								}
							}
							fmt.Println("sembast-to-badger completed")
						} else {
							fmt.Println(invalidSembastFileMessage)
						}
					} else {
						fmt.Println(invalidSembastFileMessage)
					}
				} else {
					fmt.Println(invalidSembastFileMessage)
				}
			} else {
				fmt.Println(invalidSembastFileMessage)
			}
		} else {
			fmt.Println(invalidSembastFileMessage)
		}
	} else {
		fmt.Println("Usage:")
		fmt.Println("sembast-to-badger -s \"sembast export filename\" -b \"badger directory\"")
	}
}
