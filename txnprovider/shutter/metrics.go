package shutter

import "github.com/erigontech/erigon-lib/metrics"

var (
	encryptedTxnsPoolAdded      = metrics.GetOrCreateCounter("shutter_encrypted_txns_pool_added")
	encryptedTxnsPoolDeleted    = metrics.GetOrCreateCounter("shutter_encrypted_txns_pool_deleted")
	encryptedTxnsPoolTotalCount = metrics.GetOrCreateGauge("shutter_encrypted_txns_pool_total_count")
	encryptedTxnsPoolTotalBytes = metrics.GetOrCreateGauge("shutter_encrypted_txns_pool_total_bytes")
	encryptedTxnSizeBytes       = metrics.GetOrCreateSummary("shutter_encrypted_txn_size_bytes")

	decryptedTxnsPoolAdded      = metrics.GetOrCreateCounter("shutter_decrypted_txns_pool_added")
	decryptedTxnsPoolDeleted    = metrics.GetOrCreateCounter("shutter_decrypted_txns_pool_deleted")
	decryptedTxnsPoolTotalCount = metrics.GetOrCreateGauge("shutter_decrypted_txns_pool_total_count")
	decryptedTxnsPoolTotalBytes = metrics.GetOrCreateGauge("shutter_decrypted_txns_pool_total_bytes")
	decryptedTxnSizeBytes       = metrics.GetOrCreateSummary("shutter_decrypted_txn_size_bytes")

	decryptionKeysTopicPeerCount     = metrics.GetOrCreateGauge("shutter_decryption_keys_topic_peer_count")
	decryptionKeysSlotDelaySecs      = metrics.GetOrCreateSummary("shutter_decryption_keys_slot_delay_secs")
	decryptionKeysCount              = metrics.GetOrCreateSummary("shutter_decryption_keys_count")
	decryptionKeysProcessingTimeSecs = metrics.GetOrCreateSummary("shutter_decryption_keys_processing_time_secs")
	decryptionKeysGas                = metrics.GetOrCreateSummary("shutter_decryption_keys_gas")
	decryptionKeysRejections         = metrics.GetOrCreateGauge("shutter_decryption_keys_rejections")
	decryptionKeysIgnores            = metrics.GetOrCreateGauge("shutter_decryption_keys_ignores")
	decryptionFailures               = metrics.GetOrCreateCounter("shutter_decryption_failures")
	decryptionSuccesses              = metrics.GetOrCreateCounter("shutter_decryption_successes")

	parentBlockWaitSecs    = metrics.GetOrCreateSummary("shutter_parent_block_wait_secs")
	parentBlockOnTime      = metrics.GetOrCreateCounter("shutter_parent_block_on_time")
	parentBlockMissed      = metrics.GetOrCreateCounter("shutter_parent_block_missed")
	decryptionMarkWaitSecs = metrics.GetOrCreateSummary("shutter_decryption_mark_wait_secs")
	decryptionMarkOnTime   = metrics.GetOrCreateCounter("shutter_decryption_mark_on_time")
	decryptionMarkMissed   = metrics.GetOrCreateCounter("shutter_decryption_mark_missed")
)
