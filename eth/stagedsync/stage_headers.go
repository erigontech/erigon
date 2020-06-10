package stagedsync

func SpawnHeaderDownloadStage(s *StageState, u Unwinder, d DownloaderGlue, headersFetchers []func() error) error {
	err := d.SpawnHeaderDownloadStage(headersFetchers, s, u)
	if err == nil {
		s.Done()
	}
	return err
}
