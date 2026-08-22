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
