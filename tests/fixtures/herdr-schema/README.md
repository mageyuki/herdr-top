# herdr socket-API schema baseline

`baseline.json` is the bundled socket-API schema document for the highest
reviewed herdr protocol, currently protocol 20. Its top-level keys are
`$schema`, `protocol`, `schema_version`, `schemas`, and `title`.

## Source

The fixture was extracted from herdr 0.8.2. The recorded version output is:

```text
herdr 0.8.2
```

The SHA-256 digest of the herdr binary is:

```text
976150a14d490c94b243ea2e1a7eb2dfb67f12e36b182db90936f6728e6aecf4
```

Extract the schema from the repository root with:

```bash
herdr api schema --json > tests/fixtures/herdr-schema/baseline.json
```

## Extending the reviewed protocol set

Run `scripts/review-herdr-protocol.sh` against the new herdr binary and review
the printed schema-record delta. In one change, extend `REVIEWED_HERDR_PROTOCOLS`,
replace `tests/fixtures/herdr-schema/baseline.json` with the new command's raw
output, and update this README with the new protocol, version, and binary
digest. The baseline filename is version-agnostic, so no other fixture path
changes are required.

Value and member changes inside array elements are compared as whole elements,
so they require review even when otherwise additive.

## Provider-log format baselines

`claude-log-baseline.json` and `codex-log-baseline.json` pin the multiset of
record types exercised by `tests/fixtures/provider-logs/`, plus reviewed
version prefixes `2.1.` and `0.149.`. Regenerate the Claude inventory with:

```bash
jq -s --arg description 'Pins the multiset of Claude fixture record/content types and the reviewed 2.1.x transcript version prefix. Regenerate from tests/fixtures/provider-logs as documented in README.md.' '
  {
    description: $description,
    version_prefix: "2.1.",
    record_types: ([.[]
      | {scope: "record", type: .type},
        (if (.message.content? | type) == "array" then
           .message.content[] | select((.type? | type) == "string")
           | {scope: "content", type: .type}
         else empty end)]
      | sort_by(.scope, .type)
      | group_by([.scope, .type])
      | map({scope: .[0].scope, type: .[0].type, occurrences: length}))
  }' tests/fixtures/provider-logs/claude-*.jsonl \
  > tests/fixtures/herdr-schema/claude-log-baseline.json
```

Regenerate the Codex inventory with:

```bash
jq -s --arg description 'Pins the multiset of Codex fixture record/response/event/item types and the reviewed 0.149.x CLI version prefix. Regenerate from tests/fixtures/provider-logs as documented in README.md.' '
  {
    description: $description,
    version_prefix: "0.149.",
    record_types: ([.[]
      | {scope: "record", type: .type},
        (if .type == "response_item" then
           {scope: "response_item", type: .payload.type}
         else empty end),
        (if .type == "event_msg" then
           {scope: "event_msg", type: .payload.type},
           (if .payload.type == "item_completed" then
              {scope: "item_completed", type: .payload.item.type},
              (.payload.item.content[]?
               | select((.type? | type) == "string")
               | {scope: "item_content", type: .type})
            else empty end)
         else empty end)]
      | sort_by(.scope, .type)
      | group_by([.scope, .type])
      | map({scope: .[0].scope, type: .[0].type, occurrences: length}))
  }' tests/fixtures/provider-logs/codex-*.jsonl \
  > tests/fixtures/herdr-schema/codex-log-baseline.json
```

Review the regenerated inventories and then run
`scripts/review-herdr-protocol.sh --log-baselines`. A provider minor-version
bump within the pinned prefix does not require review by itself; a new prefix
or any added or removed record occurrence does.
