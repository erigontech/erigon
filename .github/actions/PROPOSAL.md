use merge queues:
	- fixes teh issue of rerunning tests after PR is merged
	- runs tests as tho they were on the merge target, and become the new commit
	- batch multiple PRs together, reducing our speed from approximately 1 commit/1-2 hours to (with other proposed fixes), N commits/30 mins
	- reduces need to block PR completely in separate step
	- correct place to put and block on any tests that devs can reproduce locally
	- only, and can effectively, run most comprehensive tests here (like race)

change workflow use:
	- group jobs by purpose like
		- all commits
		- merge queue
		- QA
	- ensure dispatchability, but use dispatch variables (QA has this done really well in a few places)

better caching:
	- smarter go mod caching, reuse across jobs
	- constrain build caches to use case (test, coverage, specific tests)
	- set mtimes to allow test caching
	- caching for fixtures is per-package, so breaking up big packages for different fixtures fixes this

timeouts:
	- aim for individual job limits of 30 minutes (cold). real target is 10 mins average. this is not to catch overuse of runners, but to ensure tests evolve to be parallelizable as we go (PRs that make them bigger should improve the workflow to accomodate if required).
	- on merge queue, set timeouts higher (60 mins)

regressions:
	- no workflows should invalidate past success. for example lint should not run on main, it should block on PR. it probably shouldn't run in merge queue.
	- regressions should be detected asynchronously on schedules (QA do this for example).

local reproducibility:
	- all jobs should have a way to locally reproduce for testing, and dev prechecks (i'm changing X code, so i should do X tests first).

flaky tests:
	- these should be aggressively trimmed. unrelated failures in PRs should be reported, and skipped rather than waiting.
	- ideal place for bots to discover, and fix

next steps:
	- 3-5x our runner counts
	- enable merge queue, perhaps on a test branch (merge-queue-experiment/main)
