"""Reviewed public signatures and exports."""

import inspect
import json
import unittest
from pathlib import Path

import flowpipeline


class PublicContractTests(unittest.TestCase):
    def test_reviewed_public_apis_match_the_contract(self):
        contract = json.loads(
            Path(__file__).with_name("api_contract.json").read_text(encoding="utf-8")
        )
        self.assertCountEqual(flowpipeline.__all__, contract["exports"])
        for name in contract["exports"]:
            self.assertIsNotNone(getattr(flowpipeline, name))
        for name, definition in contract["signatures"].items():
            item = getattr(flowpipeline, name)
            if "$call" in definition:
                self.assertEqual(str(inspect.signature(item)), definition["$call"], name)
            for method, signature in definition["methods"].items():
                self.assertEqual(
                    str(inspect.signature(getattr(item, method))),
                    signature,
                    f"{name}.{method}",
                )
