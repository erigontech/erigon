package apis

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func RegisterStorageTombstonesAPI(router *gin.RouterGroup, e *Env) error {
	router.GET("/", e.FindStorageTombstone)
	router.GET("/integrity/", e.GetTombstoneIntegrity)
	return nil
}
func (e *Env) GetTombstoneIntegrity(c *gin.Context) {
	results, err := storageTombstonesIntegrityDBCheck(e.DB)
	if err != nil {
		c.Error(err) //nolint:errcheck
		return
	}
	c.JSON(http.StatusOK, results)
}
func (e *Env) FindStorageTombstone(c *gin.Context) {
	results, err := findStorageTombstoneByPrefix(c.Query("prefix"), e.DB)
	if err != nil {
		c.Error(err) //nolint:errcheck
		return
	}
	c.JSON(http.StatusOK, results)
}

type StorageTombsResponse struct {
	Prefix      string `json:"prefix"`
	HideStorage bool   `json:"hideStorage"`
}

func findStorageTombstoneByPrefix(prefixS string, remoteDB ethdb.KV) ([]*StorageTombsResponse, error) {
	var results []*StorageTombsResponse
	prefix := common.FromHex(prefixS)
	if err := remoteDB.View(context.TODO(), func(tx ethdb.Tx) error {
		interBucket := tx.Bucket(dbutils.IntermediateTrieHashBucket)
		c := interBucket.Cursor()
		storage := tx.Bucket(dbutils.StorageBucket).Cursor().Prefetch(1)

		for k, vSize, err := c.First(); k != nil || err != nil; k, vSize, err = c.Next() {
			if err != nil {
				return err
			}

			if vSize > 0 {
				continue
			}

			// each tomb must cover storage
			hideStorage := false
			addrHash := common.CopyBytes(k[:common.HashLength])
			storageK, _, err := storage.Seek(addrHash)
			if err != nil {
				return err
			}
			if !bytes.HasPrefix(storageK, addrHash) {
				hideStorage = false
			} else {
				incarnation := dbutils.DecodeIncarnation(storageK[common.HashLength : common.HashLength+8])
				for ; incarnation > 0; incarnation-- {
					kWithInc := dbutils.GenerateStoragePrefix(common.BytesToHash(addrHash), incarnation)
					kWithInc = append(kWithInc, k[common.HashLength:]...)
					storageK, _, err = storage.Seek(kWithInc)
					if err != nil {
						return err
					}
					if bytes.HasPrefix(storageK, kWithInc) {
						hideStorage = true
					}
				}
				if hideStorage {
					break
				}
			}

			results = append(results, &StorageTombsResponse{
				Prefix:      fmt.Sprintf("%x\n", k),
				HideStorage: hideStorage,
			})

			if len(results) > 50 {
				results = append(results, &StorageTombsResponse{
					Prefix:      "too much results",
					HideStorage: true,
				})
				return nil
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return results, nil
}

type IntegrityCheck struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func storageTombstonesIntegrityDBCheck(remoteDB ethdb.KV) ([]*IntegrityCheck, error) {
	var results []*IntegrityCheck
	return results, remoteDB.View(context.TODO(), func(tx ethdb.Tx) error {
		res, err := storageTombstonesIntegrityDBCheckTx(tx)
		if err != nil {
			return err
		}
		results = res
		return nil
	})
}

func storageTombstonesIntegrityDBCheckTx(tx ethdb.Tx) ([]*IntegrityCheck, error) {
	var res []*IntegrityCheck
	var check1 = &IntegrityCheck{
		Name:  "1 trie prefix must be covered only by 1 tombstone",
		Value: "ok",
	}
	res = append(res, check1)
	check2 := &IntegrityCheck{
		Name:  "tombstone must hide at least 1 storage",
		Value: "ok",
	}
	res = append(res, check2)

	inter := tx.Bucket(dbutils.IntermediateTrieHashBucket).Cursor().Prefetch(1000)
	storage := tx.Bucket(dbutils.StorageBucket).Cursor().Prefetch(10).NoValues()

	for k, v, err := inter.First(); k != nil || err != nil; k, v, err = inter.Next() {
		if err != nil {
			return nil, err
		}
		if len(v) > 0 {
			continue
		}

		// 1 prefix must be covered only by 1 tombstone
		pref := append(k, []byte{0, 0}...)
		c2 := tx.Bucket(dbutils.IntermediateTrieHashBucket).Cursor().Prefix(pref).Prefetch(10)
		for overlapK, overlapV, err := c2.First(); overlapK != nil || err != nil; overlapK, overlapV, err = c2.Next() {
			if err != nil {
				return nil, err
			}
			if len(overlapV) > 0 {
				continue
			}

			if bytes.HasPrefix(overlapK, k) {
				check1.Value = fmt.Sprintf("%x is prefix of %x\n", overlapK, k)
				break
			}
		}

		// each tombstone must hide at least 1 storage
		addrHash := common.CopyBytes(k[:common.HashLength])
		storageK, _, err := storage.Seek(addrHash)
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(storageK, addrHash) {
			return nil, fmt.Errorf("tombstone %x has no storage to hide\n", k)
		} else {
			incarnation := dbutils.DecodeIncarnation(storageK[common.HashLength : common.HashLength+8])
			hideStorage := false
			for ; incarnation > 0; incarnation-- {
				kWithInc := dbutils.GenerateStoragePrefix(common.BytesToHash(addrHash), incarnation)
				kWithInc = append(kWithInc, k[common.HashLength:]...)
				storageK, _, err = storage.Seek(kWithInc)
				if err != nil {
					return nil, err
				}
				if bytes.HasPrefix(storageK, kWithInc) {
					hideStorage = true
				}
			}

			if !hideStorage {
				check2.Value = fmt.Sprintf("tombstone %x has no storage to hide\n", k)
				break
			}
		}
	}
	return res, nil
}
