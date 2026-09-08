#!/bin/sh
set -eu
[ "${1:-}" != --help ] || { echo 'Usage: scripts/faults/m0_knowledge.sh [SEED]'; exit 0; }
seed=${1:-m0-knowledge-v1}; [ "$seed" = m0-knowledge-v1 ] || { echo E_SEED >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd);cd "$root"
# Every hostile validator family is run twice by the deterministic unit corpus.
for round in 1 2; do python3 -m unittest -q \
 tests.validate_knowledge.Knowledge.test_absent_index_and_exact_mismatch \
 tests.validate_knowledge.Knowledge.test_forged_owner_unowned_range_hash_and_provenance \
 tests.validate_knowledge.Knowledge.test_superseded_freshness_and_rewritten_history \
 tests.validate_knowledge.Knowledge.test_findings_append_only_and_hypothesis_separation \
 tests.validate_knowledge.Knowledge.test_handoff_redaction_ambiguity_and_separation \
 tests.validate_knowledge.Knowledge.test_hostile_paths_and_determinism >/dev/null;done
printf 'm0 knowledge hostile corpus pass seed=%s repeated=2 product_faults=fault_not_applicable\n' "$seed"
