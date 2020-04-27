package downloader

import "github.com/ledgerwatch/turbo-geth/log"

func (d *Downloader) doStagedSyncWithFetchers(p *peerConnection, headersFetchers []func() error) error {

	log.Info("Sync stage 1/5. Downloading headers...")

	var err error

	/*
	* Stage 1. Download Headers
	 */
	if err = d.spawnSync(headersFetchers); err != nil {
		return err
	}

	log.Info("Sync stage 1/5. Downloading headers... Complete!")
	log.Info("Sync stage 2/5. Downloading block bodies...")

	/*
	* Stage 2. Download Block bodies
	 */
	cont := true

	for cont && err == nil {
		cont, err = d.spawnBodyDownloadStage(p.id)
	}
	if err != nil {
		return err
	}

	log.Info("Sync stage 2/5. Downloading block bodies... Complete!")
	/*
	* Stage 3. Recover senders from tx signatures
	*/
	log.Info("Sync stage 3/5. Recovering senders from tx signatures...")

	syncHeadNumber := uint64(0)
	syncHeadNumber, err = d.spawnRecoverSendersStage()
	if err != nil {
		return err
	}

	log.Info("Sync stage 3/5. Recovering senders from tx signatures... Complete!")	
	log.Info("Sync stage 4/5. Executing blocks w/o hash checks...")

	/*
	* Stage 4. Execute block bodies w/o calculating trie roots
	 */
	syncHeadNumber = uint64(0)
	syncHeadNumber, err = d.spawnExecuteBlocksStage()
	if err != nil {
		return err
	}

	log.Info("Sync stage 4/5. Executing blocks w/o hash checks... Complete!")

	// Further stages go there
	log.Info("Sync stage 5/5. Validating final hash")
	if err = d.spawnCheckFinalHashStage(syncHeadNumber); err != nil {
		return err
	}
	log.Info("Sync stage 5/5. Validating final hash... Complete!")

	return err
}
