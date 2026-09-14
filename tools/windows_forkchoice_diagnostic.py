import json
import os
from pathlib import Path
import platform
import subprocess
import sys


ROOT = Path.cwd()
OUT = ROOT / "diagnostic-results"
OUT.mkdir(exist_ok=True)
ENV = os.environ.copy()
ENV["ERIGON_EXECUTION_TESTS_TMPDIR"] = str(OUT)
ENV["GOTRACEBACK"] = "system"
ENV["GODEBUG"] = "cgocheck=0"
ENV["GOTOOLCHAIN"] = "local"
RESULTS = []


def run(name, command, env=ENV, timeout=600):
    print(f"BEGIN {name}: {command}", flush=True)
    with (OUT / f"{name}.log").open("w", encoding="utf-8") as log:
        try:
            result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
            code = result.returncode
        except subprocess.TimeoutExpired:
            code = 124
    data = (OUT / f"{name}.log").read_text(encoding="utf-8", errors="replace")
    crash = "found pointer to free object" in data
    RESULTS.append({"name": name, "exit_code": code, "gc_crash": crash})
    (OUT / "results.json").write_text(json.dumps(RESULTS, indent=2), encoding="utf-8")
    print(f"END {name}: exit={code} gc_crash={crash}", flush=True)
    if code:
        print(data[-24000:], flush=True)
    elif name.startswith("stock") or name.startswith("reserve"):
        print(data[-2000:], flush=True)
    return code


def build(name, overlay=None):
    suffix = ".exe" if os.name == "nt" else ""
    binary = OUT / (name + suffix)
    flags = f"-c -o {binary.as_posix()}"
    if overlay:
        flags += f" -overlay={overlay.as_posix()}"
    code = run("build-" + name, ["make", "test-filtered", "GOTEST_PACKAGES=./cl/phase1/forkchoice", f"GO_FLAGS={flags}"], timeout=1200)
    if code:
        sys.exit(code)
    return binary


def test(binary, name):
    for mode in ["true", "false"]:
        env = ENV.copy()
        env["ERIGON_COMMITMENT_PARALLEL"] = mode
        run(f"{name}-parallel-{mode}", [str(binary), "-test.v", "-test.count=3", "-test.timeout=8m"], env)


print(platform.platform(), flush=True)
if os.name == "nt":
    run("cpu", ["powershell", "-NoProfile", "-Command", "Get-CimInstance Win32_Processor | Select-Object Name,Manufacturer,NumberOfCores,NumberOfLogicalProcessors | ConvertTo-Json"])
    print((OUT / "cpu.log").read_text(encoding="utf-8", errors="replace"), flush=True)
run("go-env", ["go", "env", "-json"])
stock = build("stock")
test(stock, "stock")
if any(result["gc_crash"] for result in RESULTS):
    goroot = Path(subprocess.check_output(["go", "env", "GOROOT"], text=True).strip())
    source = goroot / "src/runtime/stack.go"
    original = source.read_text(encoding="utf-8")
    old = "stackSystem = goos.IsWindows*4096"
    if original.count(old) != 1:
        raise RuntimeError("unexpected Windows stack reserve")
    patched = OUT / "stack.go"
    patched.write_text(original.replace(old, "stackSystem = goos.IsWindows*16384"), encoding="utf-8")
    overlay = OUT / "runtime-overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(source): str(patched)}}), encoding="utf-8")
    enlarged = build("reserve-16384", overlay)
    test(enlarged, "reserve-16384")
    test(stock, "stock-recheck")
else:
    print("No GC crash on this host; no causal conclusion from this sample.", flush=True)
print(json.dumps(RESULTS, indent=2), flush=True)
sys.exit(int(any(result["exit_code"] for result in RESULTS)))
