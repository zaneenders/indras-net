# M12 — Workflow, handler, and storage compatibility

Status: planned

Dependencies: [M11](11-workflow-cancellation.md)

Read the shared [delivery contract](../PLAN.md#delivery-contract) and
[evidence format](../PLAN.md#evidence-and-verification).

## Observable outcome

Recover a run written by an older supported version, preserve recorded operation results, and execute its remaining steps with the intended handler version. A new run can use a newer handler without silently changing the old run.

## Scope and boundaries

Version durable formats and commands from their introduction; this milestone
adds an end-to-end compatibility gate. Keep handler kind/version pinned when work
is accepted. The first coordinator upgrade policy is a coordinated stop,
checkpoint, validate/migrate, and restart. Old/new workers may coexist through
explicit kind matching; rolling coordinator upgrades are deferred.

## Implementation steps

- [ ] Document supported checkpoint, record, command, and response format versions.
- [ ] Keep immutable older-version fixtures and expected logical state.
- [ ] Pin handler identity/version in accepted steps; distinguish payload schema from handler kind.
- [ ] Implement explicit compatible reading or staged atomic migration for supported old state.
- [ ] Preserve durable request identity and recorded response semantics through migration.
- [ ] Reject unknown versions before participation or replacement of good local state.
- [ ] Document an executable stop/checkpoint/upgrade/restart sequence and its failure behavior.

## Runnable demo

Proposed commands; the scenario must exist before these can pass:

~~~sh
swift run indras-demo compatibility --seed 42 --output .demo/m12
swift run indras-demo verify .demo/m12
~~~

| Case | Driver action | Required observation |
| --- | --- | --- |
| Old checkpoint | Restore supported old records and retry a saved operation | Expected state/result is preserved |
| Old in-flight step | Recover a run requiring handler v1 with v1/v2 workers present | Only a compatible worker claims it |
| New run | Submit a definition pinned to handler v2 | New semantics are explicit and separate |
| Interrupted migration | Kill during staged migration | A valid old or complete new image remains recoverable |
| Unknown format | Open unsupported checkpoint/command schema | Explicit incompatibility; no silent reset |
| Full upgraded restart | Restart all nodes from validated migrated state | Logical state, deduplication, and pending work agree |

## Acceptance criteria

- [ ] Supported historical fixtures remain unchanged and independently checked.
- [ ] An old run cannot silently switch handler implementation because a worker was updated.
- [ ] Recorded successful operations do not re-execute during migration/replay.
- [ ] Schema conversion and checkpoint publication are atomic or recoverably staged.
- [ ] Unsupported state never becomes an empty database or a participating incompatible replica.
- [ ] The supported coordinator upgrade procedure is documented and exercised.
- [ ] Mixed worker versions obey explicit capability matching.

## Required evidence

Include old/new schema and handler identities, fixture hashes, migration barriers, saved retry results, and post-upgrade state digests.

## Regression and completion gate

M05/M06 identity/checkpoint recovery and M07–M11 workflow behaviors on supported fixtures.

- [ ] All listed cases and acceptance checks pass.
- [ ] Independent verification passes and rejects deliberately invalid evidence.
- [ ] Record exact commands, source identity, configuration, and run location here.
- [ ] Document public API/disk-format changes and completed simplification work.

Evidence: not yet produced.

## Out of scope

Arbitrary rolling coordinator upgrades, automatic migration of every historical format, and changing accepted step semantics in place.
