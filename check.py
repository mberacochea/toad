from pathlib import Path


def check(entry: str, launch_exitcode: int, launch_stdout: str, launch_stderr: str):
    if entry == "SRR1":
        return True, 0, "", ""
    return False, 2, "STD OUTPUT", "STD ERROR"

    # if Path(entry).exists():
    # print(f"{entry} exists")
    # return True
    # print(f"{entry} doesn't exist")
    # return False
