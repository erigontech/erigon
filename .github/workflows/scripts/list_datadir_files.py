#!/usr/bin/env python3
"""List every file in a datadir as "size_bytes<TAB>mtime_utc<TAB>path".

Paths are relative to the datadir and slash-separated, so a listing taken on
one platform reads the same on another. Sorted by path, with a header line
carrying the file and byte totals.
"""
import argparse
import datetime
import os


def collect(root):
    rows = []
    for base, _, names in os.walk(root):
        for name in names:
            path = os.path.join(base, name)
            st = os.stat(path)
            mtime = datetime.datetime.fromtimestamp(st.st_mtime, datetime.timezone.utc)
            rows.append((os.path.relpath(path, root).replace(os.sep, "/"), st.st_size,
                         mtime.strftime("%Y-%m-%dT%H:%M:%SZ")))
    return sorted(rows)


def render(rows, root):
    lines = [f"# {len(rows)} files, {sum(r[1] for r in rows)} bytes in {root}",
             "# size_bytes\tmtime_utc\tpath"]
    lines += [f"{size}\t{mtime}\t{path}" for path, size, mtime in rows]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("datadir", help="directory to list")
    parser.add_argument("output", help="file to write the listing to")
    args = parser.parse_args()

    with open(args.output, "w", encoding="utf-8") as fh:
        fh.write(render(collect(args.datadir), args.datadir))


if __name__ == "__main__":
    main()
