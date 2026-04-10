from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.safe_super_minas_enrichment import SafeSuperMinasEnricher


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=122)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    enricher = SafeSuperMinasEnricher(logger=logging.getLogger("super_minas_enrichment"))
    stats = enricher.run(limit=max(1, int(args.limit)), update_crm=not bool(args.dry_run))
    print(stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
