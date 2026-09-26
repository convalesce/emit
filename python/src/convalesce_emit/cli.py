"""
The `convalesce-emit` command: checks this machine can reach Convalesce.

Run it where the tool runs -- the Airflow scheduler and workers, the Dagster
daemon, the Prefect worker -- with the same environment the tool has. It
sends one empty batch with the configured key. The receiver accepts that
without writing anything and records that the key was used, which is what
the console watches for to say it heard from the tool.

Import as:

import convalesce_emit.cli as cecli
"""

import argparse
import gzip
import logging
import sys
from typing import Any, Dict, List, Optional, TextIO

import convalesce_emit.client as ceclient
import convalesce_emit.config as ceconfig
import convalesce_emit.errors as ceerrors

_LOG = logging.getLogger(__name__)

EXIT_CONNECTED = 0
EXIT_NOT_CONNECTED = 1
EXIT_MISCONFIGURED = 2

# Accepted and validated by the receiver, and publishes nothing.
_EMPTY_BATCH = b'{"observations":[]}'

_HINTS: Dict[int, str] = {
    401: (
        "the key was refused: it is mistyped, withdrawn, or not an ingest "
        "key for this deployment"
    ),
    403: "the key may not send observations: issue an ingest key for this tool",
    404: (
        "nothing answers at that path: for a self-hosted Convalesce, "
        "CONVALESCE_ENDPOINT must end in /openapi"
    ),
}


def main(argv: Optional[List[str]] = None) -> int:
    """
    Run the command line.

    :param argv: the arguments after the program name; the process's own
        when None
    :return: the exit code
    """
    parser = argparse.ArgumentParser(
        prog="convalesce-emit",
        description="Forward a data tool's own output to Convalesce.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    check_parser = commands.add_parser(
        "check",
        help="Check this machine can reach Convalesce with its ingest key.",
    )
    check_parser.add_argument(
        "--endpoint", help="Overrides CONVALESCE_ENDPOINT."
    )
    check_parser.add_argument(
        "--ingest-key", help="Overrides CONVALESCE_INGEST_KEY."
    )
    args = parser.parse_args(argv)
    overrides: Dict[str, Any] = {}
    if args.endpoint:
        overrides["endpoint"] = args.endpoint
    if args.ingest_key:
        overrides["ingest_key"] = args.ingest_key
    try:
        config = ceconfig.Config.from_env(**overrides)
    except ceerrors.ConfigError as exc:
        sys.stdout.write(f"Not checked: {exc}\n")
        return EXIT_MISCONFIGURED
    return check(config, sys.stdout)


def check(config: ceconfig.Config, out: TextIO) -> int:
    """
    Send one empty batch and say what came of it.

    :param config: what the tool would send with
    :param out: where to write the answer
    :return: `EXIT_CONNECTED`, `EXIT_NOT_CONNECTED`, or `EXIT_MISCONFIGURED`
        when this configuration would send nothing at all
    """
    if not config.enabled:
        out.write(
            "Not checked: CONVALESCE_ENABLED is false, so this machine sends "
            "nothing.\n"
        )
        return EXIT_MISCONFIGURED
    if config.dry_run:
        out.write(
            "Not checked: CONVALESCE_DRY_RUN is set, so this machine sends "
            "nothing. Unset it to check.\n"
        )
        return EXIT_MISCONFIGURED
    try:
        config.validate()
    except ceerrors.ConfigError as exc:
        out.write(f"Not checked: {exc}\n")
        return EXIT_MISCONFIGURED
    url = config.endpoint.rstrip("/") + ceclient.OBSERVATIONS_PATH
    out.write(f"Checking {url} with key {key_id(config.ingest_key)}\n")
    try:
        ceclient.post(config, url, gzip.compress(_EMPTY_BATCH))
    except ceerrors.TransportError as exc:
        if exc.status is None:
            out.write(
                f"Not connected: {exc}. Check the address, DNS, any proxy, and "
                "that this machine may reach it.\n"
            )
        else:
            hint = _HINTS.get(
                exc.status, f"the endpoint answered HTTP {exc.status}"
            )
            out.write(f"Not connected: {hint}.\n")
        return EXIT_NOT_CONNECTED
    out.write(
        "Connected: the key was accepted, and Convalesce now shows it as heard "
        "from.\n"
    )
    return EXIT_CONNECTED


def key_id(key: Optional[str]) -> str:
    """
    The public part of a key, safe to print.

    :param key: the key as configured
    :return: `cvl_<scope>_<id>_...`, never the secret after it
    """
    parts = (key or "").split("_", 3)
    if len(parts) == 4 and parts[0] == "cvl" and parts[2]:
        return f"cvl_{parts[1]}_{parts[2]}_..."
    return "(not a Convalesce key)"
