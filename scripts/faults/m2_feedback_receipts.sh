#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
artifact=${M2_FEEDBACK_RECEIPT_OUT:?set M2_FEEDBACK_RECEIPT_OUT to a new artifact directory}
[[ ! -e "$artifact" ]] || { echo 'E_RECEIPT_ARTIFACT_EXISTS' >&2; exit 1; }
work=$(mktemp -d /var/tmp/m2-feedback-receipts.XXXXXX)
trap 'rm -rf "$work"' EXIT INT TERM
sources=(src/m2_capture_runtime.rs src/m2_fault_status.rs src/main.rs contracts/m2/fault-status-cases.json scripts/e2e/m2_capture_runtime.sh scripts/faults/m2_fault_status.sh scripts/faults/m2_feedback_receipts.sh scripts/validate/m2_feedback_receipts.py scripts/lib/m2_feedback_receipt_evidence.py)
source_before=$(sha256sum "${sources[@]}" | sha256sum | cut -d' ' -f1)
for attempt in 1 2; do
  mkdir -p "$work/attempt-$attempt"
  if ! M2_FAULT_STATUS_FEEDBACK_ONLY=1 M2_FEEDBACK_RECEIPT_OUT="$work/attempt-$attempt" \
    scripts/faults/m2_fault_status.sh >"$work/attempt-$attempt/suite.stdout" 2>"$work/attempt-$attempt/suite.stderr"; then
    printf 'M2_FEEDBACK_RECEIPT_ATTEMPT_FAILED attempt=%s\n' "$attempt" >&2
    tail -n 100 "$work/attempt-$attempt/suite.stderr" >&2
    exit 1
  fi
done
source_after=$(sha256sum "${sources[@]}" | sha256sum | cut -d' ' -f1)
[[ "$source_before" == "$source_after" ]]
python3 scripts/lib/m2_feedback_receipt_evidence.py "$work" "$artifact" "$source_before"
python3 scripts/validate/m2_feedback_receipts.py packet "$work"
mkdir -p "$(dirname "$artifact")"
mv -f "$work" "$artifact"
printf 'M2_FEEDBACK_RECEIPTS_OK artifact=%s attempts=2 hooks=2\n' "$artifact"
