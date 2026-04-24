package report

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/erigontech/erigon/cmd/pagemon/internal/cluster"
	"github.com/erigontech/erigon/cmd/pagemon/internal/metrics"
	"github.com/erigontech/erigon/cmd/pagemon/internal/mincore"
	"github.com/erigontech/erigon/cmd/pagemon/internal/pattern"
	"github.com/erigontech/erigon/cmd/pagemon/internal/sampler"
)

// FileResult holds all derived data for one file.
type FileResult struct {
	Path     string
	FileSize int64
	Sampled  bool // huge-file stride sampling was used

	Residency []bool
	Metrics   metrics.Metrics
	Clusters  []cluster.Cluster
	Gaps      []int64
	Pattern   pattern.Pattern
	Snapshots []sampler.Snapshot // may be nil
}

// MeasureHeader is metadata for a measure/watch invocation.
type MeasureHeader struct {
	Command  string
	Duration time.Duration
}

// WriteSnapshot renders a snapshot report (no command context).
func WriteSnapshot(w io.Writer, files []FileResult) {
	fmt.Fprintln(w, "=== pagemon snapshot ===")
	for i := range files {
		writeFileResult(w, &files[i])
	}
}

// WriteMeasure renders a measure report (command + delta).
func WriteMeasure(w io.Writer, hdr MeasureHeader, files []FileResult) {
	fmt.Fprintln(w, "=== pagemon measurement ===")
	fmt.Fprintf(w, "Command:  %s\n", hdr.Command)
	fmt.Fprintf(w, "Duration: %s\n", hdr.Duration.Round(time.Millisecond))
	for i := range files {
		writeFileResult(w, &files[i])
	}
}

// WriteWatch renders a watch report (command + temporal phases).
func WriteWatch(w io.Writer, hdr MeasureHeader, files []FileResult) {
	WriteMeasure(w, hdr, files)
}

func writeFileResult(w io.Writer, r *FileResult) {
	ps := mincore.PageSize()
	nPages := (r.FileSize + ps - 1) / ps

	fmt.Fprintf(w, "\n=== File: %s ===\n", r.Path)
	sampleNote := ""
	if r.Sampled {
		sampleNote = "  (sampled at 8-page stride)"
	}
	fmt.Fprintf(w, "Size:    %s (%s pages)%s\n",
		HumanBytes(r.FileSize), humanNum(nPages), sampleNote)
	fmt.Fprintf(w, "Loaded:  %s (%s pages)\n",
		HumanBytes(r.Metrics.BytesLoaded), humanNum(r.Metrics.PagesLoaded))
	fmt.Fprintf(w, "Density: %.1f%%   Scatter: avg %.0f pages   Max gap: %s pages\n",
		r.Metrics.Density*100, r.Metrics.ScatterScore, humanNum(r.Metrics.MaxGap))

	writeClusters(w, r.Clusters, r.Gaps)
	writeTemporalPhases(w, r.Snapshots, r.Clusters)

	fmt.Fprintf(w, "\nPattern: %s\n", r.Pattern)
}

func writeClusters(w io.Writer, clusters []cluster.Cluster, gaps []int64) {
	if len(clusters) == 0 {
		fmt.Fprintln(w, "\nClusters: none")
		return
	}
	fmt.Fprintf(w, "\nClusters (%d):\n", len(clusters))
	for i, c := range clusters {
		fmt.Fprintf(w, "  %d: pages %s–%s  (%s)\n",
			i+1, humanNum(c.StartPage), humanNum(c.EndPage), HumanBytes(c.SizeBytes))
	}
	if len(gaps) > 0 {
		parts := make([]string, len(gaps))
		for i, g := range gaps {
			parts[i] = HumanBytes(g)
		}
		fmt.Fprintf(w, "Inter-cluster gaps: %s\n", strings.Join(parts, ", "))
	}
}

func writeTemporalPhases(w io.Writer, snaps []sampler.Snapshot, clusters []cluster.Cluster) {
	if len(snaps) < 2 {
		return
	}
	phases := derivePhases(snaps, clusters)
	if len(phases) == 0 {
		return
	}
	fmt.Fprintf(w, "\nTemporal phases (%d):\n", len(phases))
	for _, p := range phases {
		clusterDesc := "none"
		if len(p.ClusterIDs) > 0 {
			parts := make([]string, len(p.ClusterIDs))
			for i, id := range p.ClusterIDs {
				parts[i] = fmt.Sprintf("cluster %d", id+1)
			}
			clusterDesc = strings.Join(parts, ", ")
		}
		fmt.Fprintf(w, "  %s–%s:  %s\n",
			p.Start.Round(time.Millisecond), p.End.Round(time.Millisecond), clusterDesc)
	}
}

// derivePhases groups consecutive snapshots where the same cluster set is active.
func derivePhases(snaps []sampler.Snapshot, clusters []cluster.Cluster) []phase {
	if len(snaps) == 0 {
		return nil
	}
	type frame struct {
		at  time.Duration
		ids []int
	}
	frames := make([]frame, len(snaps))
	for i, s := range snaps {
		frames[i] = frame{at: s.At, ids: activeClusterIDs(s.Residency, clusters)}
	}

	var phases []phase
	start := frames[0].at
	cur := frames[0].ids
	for i := 1; i < len(frames); i++ {
		if !sameIDs(frames[i].ids, cur) {
			phases = append(phases, phase{Start: start, End: frames[i-1].at, ClusterIDs: cur})
			start = frames[i].at
			cur = frames[i].ids
		}
	}
	phases = append(phases, phase{Start: start, End: frames[len(frames)-1].at, ClusterIDs: cur})
	return phases
}

type phase struct {
	Start, End time.Duration
	ClusterIDs []int
}

// activeClusterIDs returns indices of clusters that have at least one loaded page in res.
func activeClusterIDs(res []bool, clusters []cluster.Cluster) []int {
	var ids []int
	for i, c := range clusters {
		end := c.EndPage
		if end >= int64(len(res)) {
			end = int64(len(res)) - 1
		}
		for p := c.StartPage; p <= end; p++ {
			if res[p] {
				ids = append(ids, i)
				break
			}
		}
	}
	return ids
}

func sameIDs(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// HumanBytes formats bytes as a human-readable string.
func HumanBytes(n int64) string {
	switch {
	case n >= 1<<40:
		return fmt.Sprintf("%.1fTB", float64(n)/(1<<40))
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// humanNum formats an integer with comma separators.
func humanNum(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	rem := len(s) % 3
	if rem > 0 {
		b.WriteString(s[:rem])
	}
	for i := rem; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}
