#!/usr/bin/env bash
# fuse_compare_diff.sh — side-by-side summary of two fuse_compare.sh runs.
#
# Usage:
#   scripts/fusefilter-bench/fuse_compare_diff.sh <csv1> <csv2>
#
# Reports peak / final / mean for each captured memory series, plus the
# ratio (csv1 / csv2) — convenient when csv1 is the baseline (legacy Writer)
# and csv2 is the candidate (WriterSharded).

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <baseline.csv> <candidate.csv>" >&2
  exit 2
fi

A="$1"
B="$2"
[[ -s "$A" ]] || { echo "empty or missing: $A" >&2; exit 2; }
[[ -s "$B" ]] || { echo "empty or missing: $B" >&2; exit 2; }

# Print a per-CSV summary plus the cross-CSV ratio.
awk -v a="$A" -v b="$B" '
  function reset() {
    n=0; rss_max=0; vsz_max=0; ha_max=0; hi_max=0; hs_max=0; sys_max=0;
    rss_last=0; vsz_last=0; ha_last=0; hi_last=0; hs_last=0; sys_last=0;
    rss_sum=0; ha_sum=0;
    elapsed_last=0; label="";
  }
  function load(path,   line, f) {
    reset();
    getline line < path; # header
    while ((getline line < path) > 0) {
      split(line, f, ",");
      if (f[5] == "") continue;
      n++;
      elapsed_last = f[3]+0; label = f[4];
      rss_last = f[5]+0; vsz_last = f[6]+0;
      ha_last = f[7]+0;  hi_last = f[8]+0; hs_last = f[9]+0; sys_last = f[10]+0;
      if (rss_last > rss_max) rss_max = rss_last;
      if (vsz_last > vsz_max) vsz_max = vsz_last;
      if (ha_last  > ha_max)  ha_max  = ha_last;
      if (hi_last  > hi_max)  hi_max  = hi_last;
      if (hs_last  > hs_max)  hs_max  = hs_last;
      if (sys_last > sys_max) sys_max = sys_last;
      rss_sum += rss_last; ha_sum += ha_last;
    }
    close(path);
  }
  function ratio(num, den) {
    if (den == 0) return "n/a";
    return sprintf("%.2fx", num / den);
  }
  function mb(x) { return sprintf("%.1f", x / 1048576); }
  BEGIN {
    load(a);
    Alabel=label; An=n; Aelapsed=elapsed_last;
    Arss_max=rss_max; Avsz_max=vsz_max; Aha_max=ha_max; Ahi_max=hi_max; Ahs_max=hs_max; Asys_max=sys_max;
    Arss_avg=(n?rss_sum/n:0); Aha_avg=(n?ha_sum/n:0);

    load(b);
    Blabel=label; Bn=n; Belapsed=elapsed_last;
    Brss_max=rss_max; Bvsz_max=vsz_max; Bha_max=ha_max; Bhi_max=hi_max; Bhs_max=hs_max; Bsys_max=sys_max;
    Brss_avg=(n?rss_sum/n:0); Bha_avg=(n?ha_sum/n:0);

    printf "metric                         %-20s %-20s ratio (A/B)\n", Alabel, Blabel;
    printf "samples                        %-20d %-20d\n", An, Bn;
    printf "elapsed_sec                    %-20d %-20d\n", Aelapsed, Belapsed;
    printf "peak_rss_mb                    %-20s %-20s %s\n", mb(Arss_max),  mb(Brss_max),  ratio(Arss_max,  Brss_max);
    printf "peak_vsz_mb                    %-20s %-20s %s\n", mb(Avsz_max),  mb(Bvsz_max),  ratio(Avsz_max,  Bvsz_max);
    printf "peak_heap_alloc_mb             %-20s %-20s %s\n", mb(Aha_max),   mb(Bha_max),   ratio(Aha_max,   Bha_max);
    printf "peak_heap_inuse_mb             %-20s %-20s %s\n", mb(Ahi_max),   mb(Bhi_max),   ratio(Ahi_max,   Bhi_max);
    printf "peak_heap_sys_mb               %-20s %-20s %s\n", mb(Ahs_max),   mb(Bhs_max),   ratio(Ahs_max,   Bhs_max);
    printf "peak_go_sys_mb                 %-20s %-20s %s\n", mb(Asys_max),  mb(Bsys_max),  ratio(Asys_max,  Bsys_max);
    printf "avg_rss_mb                     %-20s %-20s %s\n", mb(Arss_avg),  mb(Brss_avg),  ratio(Arss_avg,  Brss_avg);
    printf "avg_heap_alloc_mb              %-20s %-20s %s\n", mb(Aha_avg),   mb(Bha_avg),   ratio(Aha_avg,   Bha_avg);
  }
' </dev/null
