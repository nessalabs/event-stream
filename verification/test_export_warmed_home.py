#!/usr/bin/env python3
"""Focused tests for the review-only warmed Home exporter."""

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("export-warmed-home.py")
SPEC = importlib.util.spec_from_file_location("export_warmed_home", MODULE_PATH)
EXPORTER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(EXPORTER)


class SemanticFingerprintTests(unittest.TestCase):
    def test_changed_config_cannot_inherit_a_prior_fingerprint(self):
        environment = {
            "kind": "environment",
            "source_sha256": "source-a",
            "input_digest": "epoch-a",
            "repetitions": 3,
            "store": "memory",
            "scenario": "warmed_subscription_scale",
        }
        base = {"kind": "config", "max_page_bytes": 4096}
        changed = {"kind": "config", "max_page_bytes": 8192}

        self.assertNotEqual(
            EXPORTER.semantic_fingerprint(environment, base),
            EXPORTER.semantic_fingerprint(environment, changed),
        )

    def test_provenance_fields_do_not_change_semantic_fingerprint(self):
        first = {
            "kind": "environment",
            "source_sha256": "source-a",
            "input_digest": "epoch-a",
            "repetitions": 3,
            "store": "memory",
        }
        second = {
            **first,
            "source_sha256": "source-b",
            "input_digest": "epoch-b",
            "repetitions": 5,
        }
        config = {"kind": "config", "max_page_bytes": 4096}

        self.assertEqual(
            EXPORTER.semantic_fingerprint(first, config),
            EXPORTER.semantic_fingerprint(second, config),
        )


if __name__ == "__main__":
    unittest.main()
