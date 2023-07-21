import os
import sys
import time
import shlex
import psutil
import pathlib
import subprocess

script_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = pathlib.Path(script_dir).parent.resolve()

# assume python binary can be found via venv
# but cover difference between windows and rest
if sys.platform not in {"darwin", "linux", "linux2"}:
    entrypoint = os.path.join(project_dir, "venv", "Scripts", "python.exe")
else:
    entrypoint = os.path.join(project_dir, "venv", "bin", "python")


base_args = f"{entrypoint} -m mortgage_scraper --randomize -s csv"
args = {
    "sbab": "-t sbab",
    "ica": "-t ica --delay 0.5",
    "skandia": "-t skandia --rotate-user-agent",
    "hypoteket": "-t hypoteket",
}

full_args = {
    target: " ".join([base_args, args_partial]) for target, args_partial in args.items()
}


def scraper_process_is_running(scraper: str) -> bool:
    for process in psutil.process_iter():
        try:
            cmdline_args = " ".join(process.cmdline())
            if "python" in process.name().lower() and scraper in cmdline_args:
                return True
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    return False


def ensure_scraper_process_is_running(scraper: str):
    if not scraper_process_is_running(scraper):
        print("starting process for: ", scraper)
        command = shlex.split(full_args[scraper])
        os.chdir(project_dir)
        subprocess.Popen(
            command,
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def main():
    """Ensures one and only one scraper process is running for each provider"""
    while True:
        for scraper in args:
            ensure_scraper_process_is_running(scraper)
        time.sleep(1)


if __name__ == "__main__":
    main()
