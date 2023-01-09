import os
import signal
import subprocess
import tempfile
import time

import requests
from dagster import DagsterInstance
from dagster._core.test_utils import environ, new_cwd, wait_for_runs_to_finish
from dagster._utils import find_free_port


# E2E test that spins up "dagster start", launches a run over dagit,
# and waits for a schedule run to launch
def test_dagster_start_command():
    with tempfile.TemporaryDirectory() as tempdir:
        with environ({"DAGSTER_HOME": ""}):
            with new_cwd(tempdir):
                dagit_port = find_free_port()

                start_process = subprocess.Popen(
                    [
                        "dagster",
                        "start",
                        "-f",
                        os.path.join(
                            os.path.dirname(__file__),
                            "repo.py",
                        ),
                        "--dagit-port",
                        str(dagit_port),
                    ],
                    cwd=tempdir,
                )

                start_time = time.time()
                while True:
                    try:
                        dagit_json = requests.get(
                            f"http://localhost:{dagit_port}/dagit_info"
                        ).json()
                        if dagit_json:
                            break
                    except:
                        print("Waiting for Dagit to be ready..")

                    if time.time() - start_time > 30:
                        raise Exception("Timed out waiting for Dagit to serve requests")

                    time.sleep(1)

                instance = None

                try:
                    start_time = time.time()
                    while True:
                        if time.time() - start_time > 30:
                            raise Exception("Timed out waiting for instance files to exist")
                        if os.path.exists(os.path.join(str(tempdir), "history")):
                            break
                        time.sleep(1)

                    instance = DagsterInstance.from_config(tempdir)
                    start_time = time.time()
                    while True:
                        if len(instance.get_runs()) > 0:
                            break

                        if time.time() - start_time > 30:
                            raise Exception("Timed out waiting for run to exist")

                        time.sleep(1)

                finally:
                    start_process.send_signal(signal.SIGINT)
                    start_process.communicate()
                    if instance:
                        wait_for_runs_to_finish(instance)
                        instance.dispose()
