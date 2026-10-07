# tracegen

Builds replayable traffic for the webhook delivery system: a fixed list of events
plus the registrations that decide where each event goes. Python 3, standard library only.

```
python tracegen.py                                  # uses the variables at the top of the file
python tracegen.py A=600 B=40 E=300 F=200 events=15000 rate=200
```

Settings: `A`–`F` endpoints per profile, `N` merchants with no endpoint (default 5 × A),
`events` list sizes (comma-separated), `rate` events/second, `seed`.
Same settings and seed give identical files.

## Output: `traces/<layout>/`

| File | One row per | Columns |
|---|---|---|
| `endpoints.csv` | receiver endpoint | `endpoint` (= receiver URL `/n`), `url_path`, `profile`, `profile_name`, `owner_merchant`, `event_types` |
| `merchants.csv` | merchant | `merchant_id`, `class` (A/B/E, or N for no endpoint), `event_share`, `own_endpoint`, `notifier_endpoints`, `fraud_endpoint`, `analytics_endpoint` |
| `subscriptions.csv` | merchant → endpoint link | `merchant_id`, `endpoint`, `event_types` (pipe-separated) |
| `events_<n>_<rate>ps.csv` | event, in emit order | `seq`, `at_ms` (when to emit, from start), `event_id`, `merchant_id`, `resource_id`, `event_type`, `endpoints` (pipe-separated) |
| `summary_<n>_<rate>ps.md` | — | event mix, fan-out, load per profile, and the counts to set in the receiver and endpoint.go |

Endpoint numbering follows the receiver: all A endpoints first, then B, C, D, E, F.
The receiver's profile counts and `Numendpoints` in endpoint.go must match the layout.

The `endpoints` column in the event list is the routing result, already worked out from
`subscriptions.csv`. Use one or the other: read the column directly, or route each
event yourself from the subscription table and compare against the column.

## Traffic model

- **Merchants.** Each A, B and E endpoint belongs to its own merchant. N merchants have no
  endpoint; their events reach only analytics, and fraud if opted in. Event volume by class:
  A 70%, B 5%, E 10%, N 15%, split evenly within a class.
- **Shared endpoints.** C (fraud vendor) gets `payment.authorized` and `dispute.opened`
  from opted-in merchants: all A and B, half of E, 1 in 30 of N (about 80% of volume).
  D (analytics) gets every event from every merchant.
- **Ops notifiers.** F endpoints attach to B merchants first, then A, and receive
  `payment.failed` and `dispute.opened` from their owner.
- **Lifecycles.** Payments: 12% fail outright, the rest are authorized, 98% of those are captured.
  3% of captured payments are refunded (created → settled), 0.5% disputed (opened → won 30% / lost).
  Subscriptions renew, 5% then cancel. Payouts are rare. Refunds and disputes share their
  payment's `resource_id`, so they count for ordering.
- **Timing.** One event per tick at `rate`. Each tick emits a scheduled follow-up if one is due,
  otherwise starts a new resource, so lifecycles from different merchants interleave.
  Gaps between a resource's events are compressed (mean 20 s to capture, 60–120 s for
  refunds and disputes) so they finish inside a run. Short lists at high rates end before
  many refunds and disputes complete; the list is still valid, just truncated.
- **Checked.** Every list is verified so that no event precedes the event it depends on
  for the same resource.
