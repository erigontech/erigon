package stagedsync

type DownloaderGlue interface {
	SpawnHeaderDownloadStage(string, []func() error, *StageState, Unwinder) error
	SpawnBodyDownloadStage(string, string, *StageState, Unwinder, *PrefetchedBlocks) (bool, error)
}
