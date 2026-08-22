#!/usr/bin/env bash
# Compares a candidate herdr bundled API schema against the committed
# baseline and reports the key-path delta. Gate for extending
# REVIEWED_HERDR_PROTOCOLS (src/diagnostics/remote.rs).
#
# Usage:
#   scripts/review-herdr-protocol.sh --candidate-file SCHEMA_JSON
#   scripts/review-herdr-protocol.sh HERDR_BINARY
#
# Exit codes: 0 additive or identical; 1 review required (removed key-paths,
# or an already-reviewed protocol whose canonicalized schema differs from the
# baseline); 2 extraction or parse failure.
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
baseline="$repo_root/tests/fixtures/herdr-schema/baseline.json"

candidate_json=""
case ${1-} in
  --candidate-file)
    [[ $# -eq 2 ]] || { echo "error: --candidate-file needs a path" >&2; exit 2; }
    candidate_json=$(cat -- "$2" 2>/dev/null) || { echo "error: cannot read $2" >&2; exit 2; }
    ;;
  "")
    echo "error: pass --candidate-file FILE or a herdr binary path" >&2; exit 2
    ;;
  *)
    candidate_json=$("$1" api schema --json 2>/dev/null) || { echo "error: schema extraction from $1 failed" >&2; exit 2; }
    ;;
esac

# Use byte collation so sort/comm agree; some UTF-8 locales make comm reject
# sort's own output.
export LC_ALL=C

paths_of() {
  jq -r '[paths | map(if type == "number" then "[]" else tostring end) | join(".")] | unique | .[]' <<<"$1" 2>/dev/null
}

candidate_protocol=$(jq -er '.protocol' <<<"$candidate_json" 2>/dev/null) || { echo "error: candidate has no protocol field" >&2; exit 2; }
baseline_json=$(cat -- "$baseline") || { echo "error: baseline missing" >&2; exit 2; }
baseline_protocol=$(jq -er '.protocol' <<<"$baseline_json")

candidate_paths=$(paths_of "$candidate_json") || { echo "error: candidate is not valid JSON" >&2; exit 2; }
baseline_paths=$(paths_of "$baseline_json")

added=$(comm -13 <(sort <<<"$baseline_paths") <(sort <<<"$candidate_paths"))
removed=$(comm -23 <(sort <<<"$baseline_paths") <(sort <<<"$candidate_paths"))

echo "baseline protocol:  $baseline_protocol"
echo "candidate protocol: $candidate_protocol"
echo "added key-paths:    $(grep -c . <<<"$added" || true)"
echo "removed key-paths:  $(grep -c . <<<"$removed" || true)"
[[ -z $added ]] || { echo "--- added ---"; echo "$added"; }
[[ -z $removed ]] || { echo "--- removed ---"; echo "$removed"; }

if [[ -n $removed ]]; then
  echo "verdict: REVIEW REQUIRED (removed key-paths)" >&2
  exit 1
fi
if [[ $candidate_protocol -le $baseline_protocol ]]; then
  # Already-reviewed protocol: the whole document must be identical after
  # canonicalization (key-path sets are blind to value/type changes).
  if [[ "$(jq -S . <<<"$candidate_json")" != "$(jq -S . <<<"$baseline_json")" ]]; then
    echo "verdict: REVIEW REQUIRED (reviewed protocol drifted from baseline)" >&2
    exit 1
  fi
fi
echo "verdict: additive or identical"
