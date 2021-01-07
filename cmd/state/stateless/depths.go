package stateless

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"

	"github.com/ledgerwatch/turbo-geth/crypto"
)

func countDepths() {
	startTime := time.Now()
	db := ethdb.MustOpen("/Volumes/tb41/turbo-geth-10/geth/chaindata")
	defer db.Close()
	var occups [64]int // Occupancy of the current level
	var counts [64][17]int
	var prev [32]byte
	count := 0
	if err := db.KV().View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.HashedAccountsBucket)
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if count == 0 {
				for i := 0; i < 64; i++ {
					occups[i] = 1
				}
			} else {
				// Find the first nibble where k and prev are different
				var mask byte = 0xf0
				var i int
				for i = 0; i < 64; i++ {
					idx := i >> 1
					if (k[idx] & mask) != (prev[idx] & mask) {
						break
					}
					mask ^= 0xff
				}
				// Finalise lower nodes
				after1 := false
				for j := i + 1; j < 64; j++ {
					if occups[j] > 1 || !after1 {
						counts[j][occups[j]]++
					}
					if occups[j] == 1 {
						after1 = true
					} else {
						after1 = false
						occups[j] = 1
					}
				}
				occups[i]++
			}
			copy(prev[:], k)
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d account records\n", count)
			}
		}
		after1 := false
		for j := 0; j < 64; j++ {
			if occups[j] > 1 || !after1 {
				counts[j][occups[j]]++
			}
			if occups[j] == 1 {
				after1 = true
			} else {
				after1 = false
				occups[j] = 1
			}
		}
		fmt.Printf("Processed %d account records\n", count)
		return nil
	}); err != nil {
		panic(err)
	}
	nodes := 0
	for i := 0; i < 64; i++ {
		exists := false
		for j := 1; j <= 16; j++ {
			if counts[i][j] > 0 {
				exists = true
				break
			}
		}
		if !exists {
			break
		}
		fmt.Printf("LEVEL %d ==========================\n", i)
		for j := 1; j <= 16; j++ {
			if counts[i][j] > 0 {
				fmt.Printf("%d: %d  ", j, counts[i][j])
				nodes += counts[i][j]
			}
		}
		fmt.Printf("\n")
	}
	fmt.Printf("Total number of nodes: %d\n", nodes)
	fmt.Printf("Count depth took %s\n", time.Since(startTime))
}

func countStorageDepths() {
	startTime := time.Now()
	db := ethdb.MustOpen("/Volumes/tb41/turbo-geth-10/geth/chaindata")
	defer db.Close()
	var occups [64]int // Occupancy of the current level
	var counts [64][17]int
	var prevAddr [20]byte
	var prev [32]byte
	var accountExists bool
	var filtered int
	count := 0
	if err := db.KV().View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.HashedStorageBucket)
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			addr := k[:20]
			sameAddr := bytes.Equal(addr, prevAddr[:])
			if !sameAddr {
				copy(prevAddr[:], addr)
				v, err := tx.GetOne(dbutils.HashedStorageBucket, crypto.Keccak256(addr[:]))
				if err != nil {
					return err
				}
				accountExists = v != nil
				if !accountExists {
					filtered++
				}
			}
			// Filter out storage of non-existent accounts
			if !accountExists {
				continue
			}
			key := k[20:52]
			if count == 0 {
				for i := 0; i < 64; i++ {
					occups[i] = 1
				}
			} else {
				if sameAddr {
					// Find the first nibble where k and prev are different
					var mask byte = 0xf0
					var i int
					for i = 0; i < 64; i++ {
						idx := i >> 1
						if (key[idx] & mask) != (prev[idx] & mask) {
							break
						}
						mask ^= 0xff
					}
					// Finalise lower nodes
					after1 := false
					for j := i + 1; j < 64; j++ {
						if occups[j] > 1 || !after1 {
							counts[j][occups[j]]++
						}
						if occups[j] == 1 {
							after1 = true
						} else {
							after1 = false
							occups[j] = 1
						}
					}
					occups[i]++
				} else {
					after1 := false
					for j := 0; j < 64; j++ {
						if occups[j] > 1 || !after1 {
							counts[j][occups[j]]++
						}
						if occups[j] == 1 {
							after1 = true
						} else {
							after1 = false
							occups[j] = 1
						}
					}
				}
			}
			copy(prev[:], key)
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d storage records, filtered accounts: %d\n", count, filtered)
			}
		}
		after1 := false
		for j := 0; j < 64; j++ {
			if occups[j] > 1 || !after1 {
				counts[j][occups[j]]++
			}
			if occups[j] == 1 {
				after1 = true
			} else {
				after1 = false
				occups[j] = 1
			}
		}
		fmt.Printf("Processed %d storage records, filtered accounts: %d\n", count, filtered)
		return nil
	}); err != nil {
		panic(err)
	}
	nodes := 0
	for i := 0; i < 64; i++ {
		exists := false
		for j := 1; j <= 16; j++ {
			if counts[i][j] > 0 {
				exists = true
				break
			}
		}
		if !exists {
			break
		}
		fmt.Printf("LEVEL %d ==========================\n", i)
		for j := 1; j <= 16; j++ {
			if counts[i][j] > 0 {
				fmt.Printf("%d: %d  ", j, counts[i][j])
				nodes += counts[i][j]
			}
		}
		fmt.Printf("\n")
	}
	fmt.Printf("Total number of nodes: %d\n", nodes)
	fmt.Printf("Count depth took %s\n", time.Since(startTime))
}
