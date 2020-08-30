package stagedsync

type DownloaderGlue interface {
	SpawnHeaderDownloadStage([]func() error, *StageState, Unwinder) error
	SpawnBodyDownloadStage(string, *StageState, Unwinder, *PrefetchedBlocks) (bool, error)
}
