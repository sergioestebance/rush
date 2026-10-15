# Session API v1: an embedded host's phases, failure and removal

The session API (`rush session api --protocol 1`, `rush session events`) is
how an owner program drives hosts it started: control only, never the
transcript. This note is for whoever reads a host's state from it. The
protocol values are in `internal/host/session_api_contract.go`; what follows
is the shape of a host's life as the API tells it.

## Phases

A host is in one phase at a time, in `state.phase`, with `phaseSince` the
time it entered it.

```
host_starting ──initialize──▶ agent_starting ──confirmed──▶ ready ◀──▶ sleeping
                                   │   ▲                       │
                                   │   └──── initialize ───────┤ (wake: expected = this host)
                                   ▼                           ▼
                                 failed ◀───── start refused   stopped
```

- `host_starting`: the host process is up, no agent yet.
- `agent_starting`: the agent runs and is being asked to confirm its model,
  effort, mode and folder. Bounded: `deadline` says by when it is `ready`
  or `failed`. The owner never waits past it.
- `ready`: confirmed, settings as asked. Only here does `send` go through.
- `sleeping`: the agent has gone (its own exit, or an idle stop) and the
  host keeps the conversation; `initialize` with `expected` wakes it.
- `failed`: definite. `failure` says why, as a `RuntimeError` (`code`,
  `retry: "user"`). The agent, if there was one, is retired. A new
  `initialize` (with `expected`, as a wake) or `prepare` tries again and
  clears it.
- `stopped`: `stop` or `delete` was confirmed; the host exits.

A dead host (process gone) answers `inspect` from its last saved state:
status `stopped` or `failed` when that was its last word, `missing`
otherwise.

## Failure codes

| `failure.code`   | What happened                                                   |
|------------------|-----------------------------------------------------------------|
| `timeout`        | The agent didn't confirm within `apiReadyWithin` (2 minutes).   |
| `exited`         | The agent exited before it confirmed.                           |
| `folder_missing` | The folder it would start in is gone. Checked before any start. |
| `unsupported`    | The agent can't be run, or refused a setting.                   |
| `conflict`       | The agent confirmed another conversation, folder or settings.   |
| `permission`     | The operation ledger couldn't be written.                       |

The operation that led there (`initialize`, `prepare`) is ledgered
`rejected`: `receipt` says so, and the response carries the same code with
outcome `not_applied`. Nothing was started.

## Delete

`delete` removes a conversation's host for good: process, folder, scratch.
It names the host as `inspect` does (`conversationId`, optional `hostId`,
`metadata`), not by incarnation, so a dead, failed or never-ready host can
be removed too. A live host stops its agent and exits first; the folder goes
once its process has. Only a public host with this conversation and owner
is taken; anything else is `conflict`.

Absence is the proof: a repeat on a host already gone is `confirmed` again,
and `inspect` says `missing`. There is no ledger entry for a delete, since
the ledger goes with the folder.

## What rush's own UI does with these hosts

Nothing. A public host is listed as "External conversation" and is never
revived by the fleet (`host.Revive` skips `PublicAPI`): its owner decides
when it comes back, and rush never puts a message of its own in it.
