package downloader

import "github.com/ledgerwatch/turbo-geth/log"

func (d *Downloader) doStagedSyncWithFetchers(p *peerConnection, headersFetchers []func() error) error {

	log.Info("Sync stage 1/4. Downloading headers...")

	var err error

	/*
	* Stage 1. Download Headers
	 */
	if err = d.spawnSync(headersFetchers); err != nil {
		return err
	}

	log.Info("Sync stage 1/4. Downloading headers... Complete!")
	log.Info("Sync stage 2/4. Downloading block bodies...")

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

	log.Info("Sync stage 2/4. Downloading block bodies... Complete!")
	log.Info("Sync stage 3/4. Executing blocks w/o hash checks...")

	/*
	* Stage 3. Execute block bodies w/o calculating trie roots
	 */
	if err = d.spawnExecuteBlocksStage(); err != nil {
		return err
	}

	log.Info("Sync stage 3/4. Executing blocks w/o hash checks... Complete!")

	// Further stages go there
	log.Info("Sync stage 4/4. Validating final hash")
	if err = d.spawnCheckFinalHashStage(); err != nil {
		return err
	}
	log.Info("Sync stage 4/4. Validating final hash... NOT IMPLEMENTED")

	return err
}
