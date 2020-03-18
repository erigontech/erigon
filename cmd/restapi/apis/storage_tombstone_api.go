package apis

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

func RegisterStorageTombstonesAPI(account *gin.RouterGroup, remoteDB *remote.DB) error {
	account.GET("/", func(c *gin.Context) {
		results, err := findStorageTombstoneByPrefix(c.Query("prefix"), remoteDB)
		if err != nil {
			c.Error(err) //nolint:errcheck
			return
		}
		c.JSON(http.StatusOK, results)
	})
	return nil
}

type StorageTombsResponse struct {
	Prefix               string `json:"prefix"`
	DontOverlapOtherTomb bool   `json:"dontOverlapOtherTomb"`
	HideStorage          bool   `json:"hideStorage"`
}

func findStorageTombstoneByPrefix(prefixS string, remoteDB *remote.DB) ([]*StorageTombsResponse, error) {
	var results []*StorageTombsResponse
	prefix := common.FromHex(prefixS)
	if err := remoteDB.View(context.TODO(), func(tx *remote.Tx) error {
		interBucket := tx.Bucket(dbutils.IntermediateTrieHashBucket)
		c := interBucket.Cursor(remote.DefaultCursorOpts.PrefetchValues(true))
		cOverlap := interBucket.Cursor(remote.DefaultCursorOpts.PrefetchValues(false).PrefetchSize(1))
		storage := tx.Bucket(dbutils.StorageBucket).Cursor(remote.DefaultCursorOpts.PrefetchValues(false).PrefetchSize(1))

		for k, v, err := c.Seek(prefix); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			if !bytes.HasPrefix(k, prefix) {
				return nil
			}

			if len(v) > 0 {
				continue
			}

			// 1 prefix must be covered only by 1 tombstone
			overlap := false
			from := append(k, []byte{0, 0}...)
			for overlapK, v, err := cOverlap.Seek(from); overlapK != nil; overlapK, v, err = cOverlap.Next() {
				if err != nil {
					return err
				}
				if !bytes.HasPrefix(overlapK, from) {
					overlapK = nil
				}
				if len(v) > 0 {
					continue
				}

				if bytes.HasPrefix(overlapK, k) {
					overlap = true
				}
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
				Prefix:               fmt.Sprintf("%x\n", k),
				DontOverlapOtherTomb: !overlap,
				HideStorage:          hideStorage,
			})

			if len(results) > 50 {
				results = append(results, &StorageTombsResponse{
					Prefix:               "too much results",
					DontOverlapOtherTomb: true,
					HideStorage:          true,
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
