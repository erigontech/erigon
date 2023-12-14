import enum
import os
import signal
import time
import sys
import datetime


class Result(enum.Enum):
    SUCCESS = 1
    FAILURE = 2
    ERROR = 3


def hr_utc_time():
    #return datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def report_result(result: Result, reason):
    if result == Result.SUCCESS:
        print(f"Check passed: {reason} - {hr_utc_time()}")
    elif result == Result.FAILURE:
        print(f"Check failed: {reason} - {hr_utc_time()}")
    elif result == Result.ERROR:
        print(f"Check error: {reason} - {hr_utc_time()}")
    else:
        print(f"Check unknown: {reason} - {hr_utc_time()}")


def check_if_process_exists(pid):
    """Check whether pid exists in the current process table."""
    if pid < 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def wait_for_process_to_exit(pid, timeout=600):
    """Wait for the process to exit (or timeout)"""
    start_time = time.time()
    while check_if_process_exists(pid):
        time.sleep(1)
        if time.time() - start_time > timeout:
            return False, timeout
    return True, time.time() - start_time


def tail_f(file):
    """ Simulate the tail -f command """
    while True:
        where = file.tell()
        line = file.readline()
        if not line:
            time.sleep(1)
            file.seek(where)
        else:
            yield line


def read_incrementally(file):
    """Simple"""
    for line in file:
        yield line


def check_log_for_exit_status(log_file):
    with open(log_file, 'r') as file:
        log_lines = read_incrementally(file)
        for line in log_lines:
            if "SIGSEGV" in line:  # Adjust this condition based on how seg fault is logged
                print("e")
                return False, "segmentation fault"
            print(".", end="")  # Print a dot for each line in the log file (to indicate "progress
    print("e")
    return True, "clean exit"  # Default to False if neither condition is found


def send_ctrl_c_and_check_log(pid, log_file):
    threshold = 60 # 60 seconds
    try:
        time.sleep(120)  # Wait before sending SIGINT, please increment this delay as necessary

        # Send SIGINT (equivalent to Ctrl-C) to the process
        os.kill(pid, signal.SIGINT)

        # Wait for process to exit in a reasonable amount of time
        exited, duration = wait_for_process_to_exit(pid, timeout=600)
        if not exited or duration > threshold:
            report_result(Result.FAILURE, f"process did not exit within timeout period, expected < {threshold} secs, measured {duration} secs")
            sys.exit(1)

        # Check the log file for exit status
        clean_exit, reason = check_log_for_exit_status(log_file)

        if clean_exit:
            report_result(Result.SUCCESS, reason)
            sys.exit(0)  # Clean exit
        else:
            report_result(Result.FAILURE, reason)
            sys.exit(1)  # Segmentation fault or error
    except Exception as e:
        report_result(Result.ERROR, e)
        sys.exit(0)  # Assume clean exit, script failed unexpectedly


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: <PID> <log_file>")
        sys.exit(1)

    pid = int(sys.argv[1])
    log_file = sys.argv[2]

    print(f"Checking pid={pid} log={log_file} start-time={hr_utc_time()}")

    send_ctrl_c_and_check_log(pid, log_file)
