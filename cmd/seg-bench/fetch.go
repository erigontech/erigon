package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/erigontech/erigon/db/snapcfg"
)

// runFetch implements `seg-bench fetch`. It downloads the chain
// manifest from R2 (with the Cloudflare header from
// db/snapcfg/cdn.go), picks a representative sampling of .seg
// files, and downloads them into --out.
func runFetch(args []string) {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	chain := fs.String("chain", "mainnet", "chain name (mainnet, sepolia, holesky, ...)")
	branch := fs.String("branch", "main", "R2 branch (typically main)")
	outDir := fs.String("out", "samples", "directory to write samples into")
	perKind := fs.Int("per-kind", 3, "files to sample per record type (headers/bodies/transactions): early, middle, late")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		die("mkdir %s: %v", *outDir, err)
	}

	manifestURL := snapcfg.ChainTomlR2URL(*branch, *chain)
	manifestPath := filepath.Join(*outDir, *chain+".toml")
	fmt.Printf("Fetching manifest from %s\n", manifestURL)
	if err := downloadCloudflare(manifestURL, manifestPath); err != nil {
		die("manifest: %v", err)
	}

	// Strip "<chain>.toml" from the manifest URL to get the base prefix
	// that .seg files live under (same R2 bucket, different filenames).
	baseURL := strings.TrimSuffix(manifestURL, "/"+*chain+".toml")

	samples, err := pickSamples(manifestPath, *perKind)
	if err != nil {
		die("pick samples: %v", err)
	}
	fmt.Printf("Selected %d files (%d per kind across headers/bodies/transactions)\n", len(samples), *perKind)

	for _, name := range samples {
		out := filepath.Join(*outDir, name)
		if _, err := os.Stat(out); err == nil {
			fmt.Printf("  skip (already present): %s\n", name)
			continue
		}
		url := baseURL + "/" + name
		fmt.Printf("  fetch %s\n", name)
		if err := downloadCloudflare(url, out); err != nil {
			die("download %s: %v", name, err)
		}
	}
	fmt.Println("\nDone. Run:")
	fmt.Printf("  seg-bench run --input %s --out report.md\n", *outDir)
}

// downloadCloudflare fetches url with the R2 Cloudflare header set.
func downloadCloudflare(url, path string) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	snapcfg.InsertCloudflareHeaders(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// segFileRe matches Erigon block-snapshot filenames like
//   v1-001000-001500-headers.seg
//   v1.1-008500-009000-bodies.seg
//   v1.0-024800-024900-transactions.seg
var segFileRe = regexp.MustCompile(`(v[0-9.]+-\d+-\d+-(headers|bodies|transactions)\.seg)`)

// pickSamples reads the manifest and picks `perKind` files per kind,
// spread across the file range (early, middle, late).
func pickSamples(manifestPath string, perKind int) ([]string, error) {
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}
	byKind := map[string][]string{}
	for _, m := range segFileRe.FindAllStringSubmatch(string(data), -1) {
		byKind[m[2]] = append(byKind[m[2]], m[1])
	}
	var out []string
	for _, kind := range []string{"headers", "bodies", "transactions"} {
		files := byKind[kind]
		sort.Strings(files)
		// Dedupe (the manifest can have duplicate entries for accessor files etc.).
		files = dedupe(files)
		if len(files) == 0 {
			continue
		}
		picks := pickSpread(files, perKind)
		out = append(out, picks...)
	}
	return out, nil
}

// dedupe removes consecutive duplicates from a sorted slice.
func dedupe(in []string) []string {
	if len(in) == 0 {
		return in
	}
	out := in[:1]
	for i := 1; i < len(in); i++ {
		if in[i] != in[i-1] {
			out = append(out, in[i])
		}
	}
	return out
}

// pickSpread returns up to n items chosen evenly across the slice.
// For n=3 with a long slice that's first / middle / last; for shorter
// slices it returns what's there without duplicates.
func pickSpread(files []string, n int) []string {
	if n <= 0 || len(files) == 0 {
		return nil
	}
	if len(files) <= n {
		return files
	}
	out := make([]string, 0, n)
	seen := map[int]bool{}
	for i := 0; i < n; i++ {
		idx := (i * (len(files) - 1)) / (n - 1)
		if n == 1 {
			idx = 0
		}
		if !seen[idx] {
			seen[idx] = true
			out = append(out, files[idx])
		}
	}
	return out
}
