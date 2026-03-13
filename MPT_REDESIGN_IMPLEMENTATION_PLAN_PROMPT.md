Use the requirements in MPT_REDESIGN_REQUIREMENTS_PROMPT.md and analysis of current pain points in  MPT_ANALYSIS_ERIGON_EXPANDED.md to implement a redesigned MPT algorithm.

Important points:
1. The current MPT implementation should be left untouched. The new implementation should live alongside it in a separate package. 
2. If possible try to copy over the unit tests for the current MPT implementation so that they are used for the new MPT implementation. You might have to simplify the unit test setup because currently it has an unnecessarily complex API.
3. The new implementation will result in different snapshot format for commitment files, and also slightly different format for account snapshots files (because of the new design encoding storage root for accounts)
4. I should be able to regenerate the snapshot files by running with the --no-downloader flag.