#!/usr/bin/env bash
# Compares a candidate herdr bundled API schema against the committed
# baseline and reports the schema-record delta. Gate for extending
# REVIEWED_HERDR_PROTOCOLS (src/diagnostics/remote.rs).
#
# Usage:
#   scripts/review-herdr-protocol.sh --candidate-file SCHEMA_JSON
#   scripts/review-herdr-protocol.sh --log-baselines [FIXTURE_DIR]
#   scripts/review-herdr-protocol.sh HERDR_BINARY
#
# Exit codes: 0 schema additive/identical or log baselines matched; 1 review
# required (schema removal/change, reviewed-protocol drift, log-record drift,
# or log-version prefix mismatch); 2 extraction, parse, or I/O failure.
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
baseline="$repo_root/tests/fixtures/herdr-schema/baseline.json"
claude_log_baseline="$repo_root/tests/fixtures/herdr-schema/claude-log-baseline.json"
codex_log_baseline="$repo_root/tests/fixtures/herdr-schema/codex-log-baseline.json"

mode="schema"
candidate_json=""
log_fixture_dir=""
case ${1-} in
  --log-baselines)
    [[ $# -le 2 ]] || { echo "error: --log-baselines accepts at most one fixture directory" >&2; exit 2; }
    mode="logs"
    log_fixture_dir=${2:-"$repo_root/tests/fixtures/provider-logs"}
    ;;
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

with_occurrences() {
  sort \
    | awk '
        NR == 1 || $0 != previous { occurrence = 0 }
        { previous = $0; occurrence++; print $0 "\toccurrence:" occurrence }
      ' \
    | sort
}

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
        # Protocol is compared explicitly below; recording its value would make
        # every valid protocol bump look like a removed schema value.
        (if $path != ["protocol"]
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
    | .[]
  ' <<<"$1" 2>/dev/null | with_occurrences
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

log_baseline_records() {
  jq -er '
    .record_types as $records
    | if ($records | type) != "array" or ($records | length) == 0 then
        error("record_types must be a non-empty array")
      else
        $records[]
        | . as $record
        | if ($record | type) != "object"
            or ($record.scope | type) != "string"
            or ($record.type | type) != "string"
            or ($record.occurrences | type) != "number"
            or $record.occurrences < 1
            or ($record.occurrences | floor) != $record.occurrences
          then error("invalid record_types entry")
          else range(0; $record.occurrences)
            | "\($record.scope)\t\($record.type)"
          end
      end
  ' "$1" 2>/dev/null | with_occurrences
}

claude_log_records() {
  jq -er '
    if (.type | type) != "string" then
      error("Claude record has no string type")
    else
      "record\t" + .type,
      (if (.message.content? | type) == "array" then
         .message.content[]
         | select((.type? | type) == "string")
         | "content\t" + .type
       else empty end)
    end
  ' "$@" 2>/dev/null | with_occurrences
}

codex_log_records() {
  jq -er '
    if (.type | type) != "string" then
      error("Codex record has no string type")
    else
      "record\t" + .type,
      (if .type == "response_item" then
         if (.payload.type | type) != "string" then
           error("Codex response_item has no string payload type")
         else
           "response_item\t" + .payload.type
         end
       else empty end),
      (if .type == "event_msg" then
         if (.payload.type | type) != "string" then
           error("Codex event_msg has no string payload type")
         else
           "event_msg\t" + .payload.type,
           (if .payload.type == "item_completed" then
              if (.payload.item.type | type) != "string" then
                error("Codex item_completed has no string item type")
              else
                "item_completed\t" + .payload.item.type,
                (.payload.item.content[]?
                 | select((.type? | type) == "string")
                 | "item_content\t" + .type)
              end
            else empty end)
         end
       else empty end)
    end
  ' "$@" 2>/dev/null | with_occurrences
}

record_count() {
  grep -c . <<<"$1" || true
}

review_log_baselines() {
  local fixture_dir=$1
  [[ -d $fixture_dir ]] || { echo "error: fixture directory missing: $fixture_dir" >&2; return 2; }
  [[ -r $claude_log_baseline ]] || { echo "error: Claude log baseline missing" >&2; return 2; }
  [[ -r $codex_log_baseline ]] || { echo "error: Codex log baseline missing" >&2; return 2; }

  shopt -s nullglob
  local claude_files=("$fixture_dir"/claude-*.jsonl)
  local codex_files=("$fixture_dir"/codex-*.jsonl)
  shopt -u nullglob
  (( ${#claude_files[@]} > 0 )) || { echo "error: no Claude JSONL fixtures in $fixture_dir" >&2; return 2; }
  (( ${#codex_files[@]} > 0 )) || { echo "error: no Codex JSONL fixtures in $fixture_dir" >&2; return 2; }

  local claude_candidate_records codex_candidate_records
  local claude_baseline_records codex_baseline_records
  claude_candidate_records=$(claude_log_records "${claude_files[@]}") || { echo "error: invalid Claude JSONL fixture" >&2; return 2; }
  codex_candidate_records=$(codex_log_records "${codex_files[@]}") || { echo "error: invalid Codex JSONL fixture" >&2; return 2; }
  claude_baseline_records=$(log_baseline_records "$claude_log_baseline") || { echo "error: invalid Claude log baseline" >&2; return 2; }
  codex_baseline_records=$(log_baseline_records "$codex_log_baseline") || { echo "error: invalid Codex log baseline" >&2; return 2; }

  local claude_prefix codex_prefix
  claude_prefix=$(jq -er '.version_prefix | select(type == "string" and length > 0)' "$claude_log_baseline" 2>/dev/null) || { echo "error: invalid Claude version prefix" >&2; return 2; }
  codex_prefix=$(jq -er '.version_prefix | select(type == "string" and length > 0)' "$codex_log_baseline" 2>/dev/null) || { echo "error: invalid Codex version prefix" >&2; return 2; }

  local claude_versions="" codex_versions=""
  local claude_file codex_file file_versions
  for claude_file in "${claude_files[@]}"; do
    jq -se 'all(.[] | select(has("version")); (.version | type) == "string")' \
      "$claude_file" >/dev/null 2>&1 \
      || { echo "error: Claude transcript version must be a string" >&2; return 2; }
    file_versions=$(jq -r 'select(has("version")) | .version' "$claude_file" 2>/dev/null | sort -u) \
      || { echo "error: invalid Claude transcript version" >&2; return 2; }
    # queue-operation-only fixtures mirror records that do not carry a
    # transcript version. Every fixture containing transcript records must
    # carry its own version evidence rather than relying on another file.
    if jq -se 'any(.[]; .type == "user" or .type == "assistant")' \
        "$claude_file" >/dev/null 2>&1 \
        && [[ -z $file_versions ]]; then
      echo "error: no Claude transcript version found in ${claude_file##*/}" >&2
      return 2
    fi
    [[ -z $file_versions ]] || claude_versions+="$file_versions"$'\n'
  done
  claude_versions=$(sort -u <<<"${claude_versions%$'\n'}")

  for codex_file in "${codex_files[@]}"; do
    jq -se '
      all(.[]
        | select(.type == "session_meta" and (.payload | has("cli_version")));
        (.payload.cli_version | type) == "string")
    ' "$codex_file" >/dev/null 2>&1 \
      || { echo "error: Codex cli_version must be a string" >&2; return 2; }
    file_versions=$(jq -r 'select(.type == "session_meta") | .payload.cli_version | select(type == "string")' "$codex_file" 2>/dev/null | sort -u) \
      || { echo "error: invalid Codex cli_version" >&2; return 2; }
    if jq -se 'any(.[]; .type == "session_meta")' \
        "$codex_file" >/dev/null 2>&1 \
        && [[ -z $file_versions ]]; then
      echo "error: no Codex cli_version found in ${codex_file##*/}" >&2
      return 2
    fi
    [[ -z $file_versions ]] || codex_versions+="$file_versions"$'\n'
  done
  codex_versions=$(sort -u <<<"${codex_versions%$'\n'}")
  [[ -n $claude_versions ]] || { echo "error: no Claude transcript version found" >&2; return 2; }
  [[ -n $codex_versions ]] || { echo "error: no Codex cli_version found" >&2; return 2; }

  local candidate_records baseline_records
  candidate_records=$(
    sed $'s/^/claude\t/' <<<"$claude_candidate_records"
    sed $'s/^/codex\t/' <<<"$codex_candidate_records"
  )
  baseline_records=$(
    sed $'s/^/claude\t/' <<<"$claude_baseline_records"
    sed $'s/^/codex\t/' <<<"$codex_baseline_records"
  )

  local added removed
  added=$(comm -13 <(sort <<<"$baseline_records") <(sort <<<"$candidate_records"))
  removed=$(comm -23 <(sort <<<"$baseline_records") <(sort <<<"$candidate_records"))

  local version_mismatches="" version
  while IFS= read -r version; do
    [[ ${version:0:${#claude_prefix}} == "$claude_prefix" ]] \
      || version_mismatches+=$'claude\t'"version:$version"$'\t'"expected-prefix:$claude_prefix"$'\n'
  done <<<"$claude_versions"
  while IFS= read -r version; do
    [[ ${version:0:${#codex_prefix}} == "$codex_prefix" ]] \
      || version_mismatches+=$'codex\t'"version:$version"$'\t'"expected-prefix:$codex_prefix"$'\n'
  done <<<"$codex_versions"
  version_mismatches=${version_mismatches%$'\n'}

  echo "baseline log records:  $(record_count "$baseline_records")"
  echo "fixture log records:   $(record_count "$candidate_records")"
  echo "added log records:     $(record_count "$added")"
  echo "removed log records:   $(record_count "$removed")"
  echo "version mismatches:    $(record_count "$version_mismatches")"
  echo "Claude version prefix: $claude_prefix (observed: $(tr '\n' ',' <<<"$claude_versions" | sed 's/,$//'))"
  echo "Codex version prefix:  $codex_prefix (observed: $(tr '\n' ',' <<<"$codex_versions" | sed 's/,$//'))"
  [[ -z $added ]] || { echo "--- added ---"; print_records "$added"; }
  [[ -z $removed ]] || { echo "--- removed ---"; print_records "$removed"; }
  [[ -z $version_mismatches ]] || { echo "--- version mismatches ---"; print_records "$version_mismatches"; }

  if [[ -n $added || -n $removed || -n $version_mismatches ]]; then
    echo "verdict: REVIEW REQUIRED (provider log format drift)" >&2
    return 1
  fi
  echo "verdict: provider log baselines match"
}

if [[ $mode == "logs" ]]; then
  review_log_baselines "$log_fixture_dir"
  exit $?
fi

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
