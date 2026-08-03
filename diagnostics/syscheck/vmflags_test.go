package syscheck

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const smapsSample = `55d0f4a00000-55d0f4a21000 r-xp 00000000 08:01 1000    /usr/bin/erigon
Size:                132 kB
Rss:                 132 kB
VmFlags: rd ex mr mw me dw
7f1000000000-7f1000200000 r--s 00000000 08:01 2001    /data/snapshots/domain/v1-accounts.0-2048.kv
Size:               2048 kB
Rss:                1024 kB
VmFlags: rd mr mw me ms rr
7f1000200000-7f1000400000 r--s 00000000 08:01 2002    /data/snapshots/domain/v1-storage.0-2048.kv
Size:               2048 kB
Rss:                 512 kB
VmFlags: rd mr mw me ms sr
7f1000400000-7f1000600000 r--s 00000000 08:01 2003    /data/snapshots/domain/v1-commitment.0-2048.kv
Size:               2048 kB
Rss:                 256 kB
VmFlags: rd mr mw me ms
7f1000600000-7f1000601000 rw-p 00000000 00:00 0
Size:                  4 kB
Rss:                   4 kB
VmFlags: rd wr mr mw me ac
`

func TestParseSmaps(t *testing.T) {
	got, err := parseSmaps(strings.NewReader(smapsSample))
	require.NoError(t, err)

	// anonymous mappings carry no path and are dropped
	require.Len(t, got, 4)

	require.Equal(t, "/usr/bin/erigon", got[0].Path)
	require.Equal(t, uint64(0x55d0f4a00000), got[0].Start)
	require.Equal(t, uint64(0x55d0f4a21000), got[0].End)
	require.False(t, got[0].Random)
	require.False(t, got[0].Sequential)

	require.True(t, got[1].Random, "rr must decode as MADV_RANDOM")
	require.False(t, got[1].Sequential)

	require.True(t, got[2].Sequential, "sr must decode as MADV_SEQUENTIAL")
	require.False(t, got[2].Random)

	require.False(t, got[3].Random, "no rr/sr is MADV_NORMAL")
	require.False(t, got[3].Sequential)
}

func TestParseSmapsPathWithSpaces(t *testing.T) {
	const s = `7f1000000000-7f1000200000 r--s 00000000 08:01 2001    /data/my snapshots/v1-accounts.0-2048.kv
VmFlags: rd mr mw me ms rr
`
	got, err := parseSmaps(strings.NewReader(s))
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "/data/my snapshots/v1-accounts.0-2048.kv", got[0].Path)
}

func TestParseSmapsSkipsPseudoPaths(t *testing.T) {
	const s = `7f1000000000-7f1000200000 rw-p 00000000 00:00 0    [heap]
VmFlags: rd wr mr mw me ac
7f1000200000-7f1000400000 r--s 00000000 08:01 2001    /data/v1-accounts.0-2048.kv
VmFlags: rd mr mw me ms rr
`
	got, err := parseSmaps(strings.NewReader(s))
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "/data/v1-accounts.0-2048.kv", got[0].Path)
}

func TestNonRandom(t *testing.T) {
	all, err := parseSmaps(strings.NewReader(smapsSample))
	require.NoError(t, err)

	got := nonRandom(all, "/data/snapshots")
	require.Len(t, got, 2)
	require.Equal(t, "/data/snapshots/domain/v1-storage.0-2048.kv", got[0].Path)
	require.True(t, got[0].Sequential)
	require.Equal(t, "/data/snapshots/domain/v1-commitment.0-2048.kv", got[1].Path)
	require.False(t, got[1].Sequential)
}

func TestParseSmapsRss(t *testing.T) {
	got, err := parseSmaps(strings.NewReader(smapsSample))
	require.NoError(t, err)

	// Rss is what the mapping actually holds in page cache; Size is only the file.
	require.Equal(t, uint64(132*1024), got[0].RssBytes)
	require.Equal(t, uint64(1024*1024), got[1].RssBytes)
	require.Equal(t, uint64(512*1024), got[2].RssBytes)
	require.Equal(t, uint64(256*1024), got[3].RssBytes)
}

func TestNonRandomRanksByRss(t *testing.T) {
	all, err := parseSmaps(strings.NewReader(smapsSample))
	require.NoError(t, err)

	got := nonRandom(all, "")
	require.Len(t, got, 3) // erigon binary + storage + commitment
	// storage 512 kB > commitment 256 kB > binary 132 kB — biggest offender first,
	// not file order.
	require.Equal(t, "/data/snapshots/domain/v1-storage.0-2048.kv", got[0].Path)
	require.Equal(t, "/data/snapshots/domain/v1-commitment.0-2048.kv", got[1].Path)
	require.Equal(t, "/usr/bin/erigon", got[2].Path)
}

func TestFileMappings(t *testing.T) {
	got, err := FileMappings()
	require.NoError(t, err)
	if runtime.GOOS != "linux" {
		require.Nil(t, got, "VmFlags is a Linux-only interface")
		return
	}
	require.NotEmpty(t, got)
	var haveSelf bool
	self, err := os.Executable()
	require.NoError(t, err)
	for _, m := range got {
		require.NotEmpty(t, m.Path)
		require.Greater(t, m.End, m.Start)
		if m.Path == self {
			haveSelf = true
		}
	}
	require.True(t, haveSelf, "the test binary must appear among its own file mappings")
}

func TestServeFileMappings(t *testing.T) {
	rec := httptest.NewRecorder()
	ServeFileMappings(rec, httptest.NewRequest(http.MethodGet, "/debug/mmap?prefix=/nonexistent", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var got struct {
		Supported bool        `json:"supported"`
		Total     int         `json:"total"`
		NonRandom []PathGroup `json:"nonRandom"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, runtime.GOOS == "linux", got.Supported)
	require.Empty(t, got.NonRandom, "no mapping lives under /nonexistent")
}

const smapsTwoVMAsOneFile = `7f1000000000-7f1000200000 r--s 00000000 08:01 2001    /data/snap/v1-accounts.0-2048.kv
Rss:                1024 kB
VmFlags: rd mr mw me ms rr
7f1000200000-7f1000400000 r--s 00000000 08:01 2001    /data/snap/v1-accounts.0-2048.kv
Rss:                 256 kB
VmFlags: rd mr mw me ms sr
7f1000400000-7f1000600000 r--s 00000000 08:01 2002    /data/snap/v1-storage.0-2048.kv
Rss:                8192 kB
VmFlags: rd mr mw me ms rr
`

func TestNonRandomGroupsReportsEveryMappingsAdvice(t *testing.T) {
	all, err := parseSmaps(strings.NewReader(smapsTwoVMAsOneFile))
	require.NoError(t, err)

	got := nonRandomGroups(all, "/data/snap")
	// storage is fully random — not an offender, and not reported at all
	require.Len(t, got, 1)
	require.Equal(t, "/data/snap/v1-accounts.0-2048.kv", got[0].Path)

	// Both VMAs are listed in address order, each with its own Rss: the two
	// mappings of one file hold different resident sets, so one summed number
	// would hide which mapping is holding the memory.
	require.Len(t, got[0].Mappings, 2)
	require.True(t, got[0].Mappings[0].Random)
	require.Equal(t, uint64(1024*1024), got[0].Mappings[0].RssBytes)
	require.True(t, got[0].Mappings[1].Sequential)
	require.Equal(t, uint64(256*1024), got[0].Mappings[1].RssBytes)

	require.Equal(t, uint64(1024*1024+256*1024), got[0].RssBytes, "group total is for ranking only")
	require.Equal(t, "random=1024kB,sequential=256kB", got[0].Advices())
}

func TestNonRandomGroupsRanksByRss(t *testing.T) {
	all, err := parseSmaps(strings.NewReader(smapsSample))
	require.NoError(t, err)

	got := nonRandomGroups(all, "")
	require.Len(t, got, 3)
	require.Equal(t, "/data/snapshots/domain/v1-storage.0-2048.kv", got[0].Path)
	require.Equal(t, "sequential=512kB", got[0].Advices())
	require.Equal(t, "/data/snapshots/domain/v1-commitment.0-2048.kv", got[1].Path)
	require.Equal(t, "normal=256kB", got[1].Advices())
	require.Equal(t, "/usr/bin/erigon", got[2].Path)
	require.Equal(t, "normal=132kB", got[2].Advices())
}
