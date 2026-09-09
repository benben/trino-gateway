#!/usr/bin/env python3
"""Run an explicitly selected disposable-lab contract in a safe, fixed order."""

import argparse
import base64
import os
import unittest


SUITES = {
    "normal": ["test_gateway.BaselineControls", "test_gateway.TransactionContract",
               "test_adversarial", "test_protocol_boundaries", "test_capabilities",
               "test_cross_group", "test_reincarnation"],
    "fault": ["test_capabilities", "test_faults.DatabaseFaultContract",
              "test_partial_cancel", "test_faults.FaultContract"],
    "real": ["test_real_trino.RealBaselineControls", "test_real_trino.RealTransactionContract"],
}


def cases(suite):
    for case in suite:
        if isinstance(case, unittest.TestSuite):
            yield from cases(case)
        else:
            yield case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, choices=SUITES)
    parser.add_argument("--list", action="store_true", help="List assertions without contacting any fixture")
    args = parser.parse_args()
    suite = unittest.defaultTestLoader.loadTestsFromNames(SUITES[args.suite])
    if args.list:
        for case in cases(suite):
            print(case.id())
        print(f"{suite.countTestCases()} tests")
        return 0
    if os.environ.get("TX_ALLOW_FIXTURE_MUTATION") != "yes":
        parser.error("Only an authorized disposable fixture may run these contracts")
    if args.suite == "fault" and os.environ.get("TX_ALLOW_IRREVERSIBLE_FAULTS") != "yes":
        parser.error("Fault tests require explicit irreversible-fault authorization")
    if not os.environ.get("TX_QUERY_AUTHORIZATION") and os.environ.get("TX_TRINO_USER") and os.environ.get("TX_TRINO_PASSWORD"):
        credential = (os.environ["TX_TRINO_USER"] + ":" + os.environ["TX_TRINO_PASSWORD"]).encode()
        os.environ["TX_QUERY_AUTHORIZATION"] = "Basic " + base64.b64encode(credential).decode()
    print(f"Running {suite.countTestCases()} {args.suite} tests; stop on the first failure", flush=True)
    return 0 if unittest.TextTestRunner(verbosity=2, failfast=True).run(suite).wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
