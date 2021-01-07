package apis

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func RegisterAccountAPI(router *gin.RouterGroup, e *Env) error {
	router.GET(":accountID", e.GetAccount)
	return nil
}

func (e *Env) GetAccount(c *gin.Context) {
	account, err := findAccountByID(c.Param("accountID"), e.KV)
	if err == ErrEntityNotFound {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"message": "account not found"})
		return
	} else if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck
		return
	}
	c.JSON(http.StatusOK, jsonifyAccount(account))
}

func jsonifyAccount(account *accounts.Account) map[string]interface{} {
	result := map[string]interface{}{
		"nonce":     account.Nonce,
		"balance":   account.Balance.ToBig().String(),
		"root_hash": account.Root.Hex(),
		"code_hash": account.CodeHash.Hex(),
		"implementation": map[string]interface{}{
			"incarnation": account.Incarnation,
		},
	}

	return result
}

func findAccountByID(accountID string, remoteDB ethdb.KV) (*accounts.Account, error) {

	possibleKeys := getPossibleKeys(accountID)

	var account *accounts.Account

	err := remoteDB.View(context.TODO(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.HashedAccountsBucket)

		for _, key := range possibleKeys {
			_, accountRlp, err := c.SeekExact(key)
			if len(accountRlp) == 0 {
				continue
			}
			if err != nil {
				return err
			}

			account = &accounts.Account{}
			return account.DecodeForStorage(accountRlp)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if account == nil {
		return nil, ErrEntityNotFound
	}

	return account, nil
}

func getPossibleKeys(accountID string) [][]byte {
	address := common.FromHex(accountID)
	addressHash, _ := common.HashData(address[:])
	return [][]byte{
		addressHash[:],
		address,
	}
}
