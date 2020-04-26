package downloader

import "github.com/ledgerwatch/turbo-geth/log"

func (d *Downloader) doStagedSyncWithFetchers(p *peerConnection, headersFetchers []func() error) error {

	log.Info("Stage 1/3. Downloading headers...")

	var err error

	/*
	* Stage 1. Download Headers
	 */
	if err = d.spawnSync(headersFetchers); err != nil {
		return err
	}

	log.Info("Stage 1/3. Downloading headers... Complete!")
	log.Info("Stage 2/3. Downloading block bodies...")

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

	log.Info("Stage 2/3. Downloading block bodies... Complete!")
	log.Info("Stage 3/3. Executing blocks w/o hash checks...")

	/*
	* Stage 3. Execute block bodies w/o calculating trie roots
	 */
	// Further stages to go here

	if err = d.spawnExecuteBlocksStage(); err != nil {
		return err
	}

	log.Info("Stage 3/3. Executing blocks w/o hash checks... Complete!")

	return err
}
