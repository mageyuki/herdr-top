#!/usr/bin/env bash
# Compares a candidate herdr bundled API schema against the committed
# baseline and reports the schema-record delta. Gate for extending
# REVIEWED_HERDR_PROTOCOLS (src/diagnostics/remote.rs).
#
# Usage:
#   scripts/review-herdr-protocol.sh --candidate-file SCHEMA_JSON
#   scripts/review-herdr-protocol.sh HERDR_BINARY
#
# Exit codes: 0 additive or identical; 1 review required (removed or changed
# schema records, or an already-reviewed protocol whose canonicalized schema
# differs from the baseline); 2 extraction or parse failure.
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

records_of() {
  jq -r '
    def canon:
      walk(if type == "object" then to_entries | sort_by(.key) | from_entries else . end);
    def path_name($path):
      $path | map(if type == "number" then "[]" else tostring end) | join(".");
    [
      paths as $path
      | getpath($path) as $value
      | "\(path_name($path))\ttype:\($value | type)",
        (if ($path[-1] | type) == "number"
            and ($value | type) != "array"
            and ($value | type) != "object"
         then "\(path_name($path))\tvalue:\($value | tojson)"
         else empty
         end),
        (if ($path[-1] | type) == "number"
            and (($value | type) == "array" or ($value | type) == "object")
         then "\(path_name($path))\tmember:\($value | canon | tojson)"
         else empty
         end)
    ]
    | unique
    | .[]
  ' <<<"$1" 2>/dev/null
}

print_records() {
  while IFS= read -r record; do
    if (( ${#record} > 200 )); then
      printf '%.200s...\n' "$record"
    else
      printf '%s\n' "$record"
    fi
  done <<<"$1"
}

candidate_protocol=$(jq -er '.protocol' <<<"$candidate_json" 2>/dev/null) || { echo "error: candidate has no protocol field" >&2; exit 2; }
baseline_json=$(cat -- "$baseline") || { echo "error: baseline missing" >&2; exit 2; }
baseline_protocol=$(jq -er '.protocol' <<<"$baseline_json" 2>/dev/null) || { echo "error: baseline has no protocol field" >&2; exit 2; }
[[ $candidate_protocol =~ ^[0-9]+$ ]] || { echo "error: candidate protocol must be a non-negative integer" >&2; exit 2; }
[[ $baseline_protocol =~ ^[0-9]+$ ]] || { echo "error: baseline protocol must be a non-negative integer" >&2; exit 2; }

candidate_records=$(records_of "$candidate_json") || { echo "error: candidate is not valid JSON" >&2; exit 2; }
baseline_records=$(records_of "$baseline_json") || { echo "error: baseline is not valid JSON" >&2; exit 2; }

added=$(comm -13 <(sort <<<"$baseline_records") <(sort <<<"$candidate_records"))
removed=$(comm -23 <(sort <<<"$baseline_records") <(sort <<<"$candidate_records"))

echo "baseline protocol:  $baseline_protocol"
echo "candidate protocol: $candidate_protocol"
echo "added schema records:   $(grep -c . <<<"$added" || true)"
echo "removed schema records: $(grep -c . <<<"$removed" || true)"
[[ -z $added ]] || { echo "--- added ---"; print_records "$added"; }
[[ -z $removed ]] || { echo "--- removed ---"; print_records "$removed"; }

if [[ -n $removed ]]; then
  echo "verdict: REVIEW REQUIRED (removed or changed schema records)" >&2
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
