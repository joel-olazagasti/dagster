import argparse
from functools import reduce
from itertools import groupby
import json
import shlex
import sys
import subprocess
from typing import Sequence, cast
from typing_extensions import NotRequired, TypedDict

parser = argparse.ArgumentParser(
    prog="run-pyright",
    description="Run pyright for every defined pyright configuration and print the merged results.",
)
parser.add_argument(
    "paths", type=str, nargs="+", help="Paths to packages containing a pyright config."
)

# ########################
# ##### TYPES
# ########################


class Position(TypedDict):
    line: int
    character: int


class Range(TypedDict):
    start: Position
    end: Position


class Diagnostic(TypedDict):
    file: str
    message: str
    severity: str
    range: Range
    rule: NotRequired[str]


class Summary(TypedDict):
    filesAnalyzed: int
    errorCount: int
    warningCount: int
    informationCount: int
    timeInSec: float


class PyrightOutput(TypedDict):
    version: str
    time: str
    generalDiagnostics: Sequence[Diagnostic]
    summary: Summary


class RunResult(TypedDict):
    returncode: int
    output: PyrightOutput


# ########################
# ##### LOGIC
# ########################

# Filter out anything not JSON
AWK_SCRIPT = r"/^{/ { JSON = 1 }; /^}/ { print; JSON = 0 }; { if (JSON) print }"


def run_pyright(path) -> RunResult:
    pipefail_cmd = "set -o pipefail"
    tox_cmd = f"tox -qqq -c {path} -e pyright -- --outputjson --level=warning"
    awk_cmd = f"awk {shlex.quote(AWK_SCRIPT)} -"
    shell_cmd = f"{pipefail_cmd} && {tox_cmd} | {awk_cmd}"
    result = subprocess.run(shell_cmd, capture_output=True, shell=True, text=True)
    return {
        "returncode": result.returncode,
        "output": cast(PyrightOutput, json.loads(result.stdout)),
    }


def merge_pyright_results(result_1: RunResult, result_2: RunResult) -> RunResult:
    returncode = 1 if 1 in (result_1["returncode"], result_2["returncode"]) else 0
    output_1, output_2 = (result["output"] for result in (result_1, result_2))
    summary = {}
    for key in output_1["summary"].keys():
        summary[key] = output_1["summary"][key] + output_2["summary"][key]
    diagnostics = [*output_1["generalDiagnostics"], *output_2["generalDiagnostics"]]
    return {
        "returncode": returncode,
        "output": {
            "time": output_1["time"],
            "version": output_1["version"],
            "summary": cast(Summary, summary),
            "generalDiagnostics": diagnostics,
        },
    }


def print_report(result: RunResult) -> None:
    output = result["output"]
    diags = sorted(output["generalDiagnostics"], key=lambda diag: diag["file"])

    print()  # blank line makes it more readable when run from `make`

    # diagnostics
    for file, file_diags in groupby(diags, key=lambda diag: diag["file"]):
        print(f"{file}:")
        for x in file_diags:
            range_str = f"{x['range']['start']['line'] + 1}:{x['range']['start']['character']}"
            head_str = f"  {range_str}: {x['message']}"
            rule_str = f"({x['rule']})" if "rule" in x else None
            full_str = " ".join(filter(None, (head_str, rule_str)))
            print(full_str + "\n")  # extra blank line for readability

    # summary
    summary = output["summary"]
    print(f"pyright {output['version']}")
    print(f"Finished in {summary['timeInSec']} seconds")
    print(f"Analyzed {summary['filesAnalyzed']} files")
    print(f"Found {summary['errorCount']} errors")
    print(f"Found {summary['warningCount']} warnings")


if __name__ == "__main__":
    args = parser.parse_args()
    run_results = [run_pyright(path) for path in args.paths]
    merged_result = reduce(merge_pyright_results, run_results)
    print_report(merged_result)
    sys.exit(merged_result["returncode"])
