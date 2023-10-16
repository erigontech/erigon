package metrics

import (
	"fmt"
	"os"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"
	"github.com/shirou/gopsutil/v3/process"
)

var rlimit2str = map[int32]string{
	process.RLIMIT_CPU:        "cpu",
	process.RLIMIT_FSIZE:      "fsize",
	process.RLIMIT_DATA:       "data",
	process.RLIMIT_STACK:      "stack",
	process.RLIMIT_CORE:       "core",
	process.RLIMIT_RSS:        "rss",
	process.RLIMIT_NPROC:      "nproc",
	process.RLIMIT_NOFILE:     "nofile",
	process.RLIMIT_MEMLOCK:    "memlock",
	process.RLIMIT_AS:         "as",
	process.RLIMIT_LOCKS:      "locks",
	process.RLIMIT_SIGPENDING: "sigpending",
	process.RLIMIT_MSGQUEUE:   "msgqueue",
	process.RLIMIT_NICE:       "nice",
	process.RLIMIT_RTPRIO:     "prtrio",
	process.RLIMIT_RTTIME:     "rttime",
}

func PrintRlimit(logger log.Logger) error {
	checkPid := os.Getpid()
	pr, err := process.NewProcess(int32(checkPid))
	if err != nil {
		return err
	}
	rl, err := pr.RlimitUsage(true)
	if err != nil {
		return err
	}
	for _, rll := range rl {
		if rll.Used < 1000 {
			continue
		}
		logger.Info(fmt.Sprintf("[dbg] %s: hard=%s, soft=%s, used=%s, %d%%\n", rlimit2str[rll.Resource], datasize.ByteSize(rll.Hard).String(), datasize.ByteSize(rll.Soft).String(), datasize.ByteSize(rll.Used).String(), rll.Used/rll.Soft))
	}
	return nil
}
