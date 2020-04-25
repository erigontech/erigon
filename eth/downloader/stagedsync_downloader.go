package downloader

import "github.com/ledgerwatch/turbo-geth/log"

func (d *Downloader) doStagedSyncWithFetchers(p *peerConnection, headersFetchers []func() error) error {
	var err error
	/*
	* Stage 1. Download Headers
	 */
	if err = d.spawnSync(headersFetchers); err != nil {
		return err
	}

	log.Info("Finished headers downloading, moving onto the the block downloading stage")

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

	/*
	* Stage 3. Execute block bodies w/o calculating trie roots
	 */
	// Further stages to go here

	if err = d.spawnExecuteBlocksStage(); err != nil {
		return err
	}

	return err
}
