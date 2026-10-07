#!/usr/bin/env python3
"""
tracegen.py - builds replayable webhook traffic for the Kifaru Pay delivery system.

Set the endpoint counts below to match the receiver, run `python tracegen.py`,
and it writes one folder per endpoint layout:

    traces/A30_B2_C1_D1_E15_F10_N150/
        endpoints.csv         one row per receiver endpoint, in receiver URL order
        merchants.csv         every merchant, its class, event share and endpoints
        subscriptions.csv     routing table: merchant + endpoint -> event types
        events_<n>_<rate>ps.csv   the event list, one file per size in EVENT_COUNTS
        summary_<n>_<rate>ps.md   event mix, fan-out and per-endpoint load of that list

Anything can be overridden on the command line without editing the file:

    python tracegen.py A=600 B=40 E=300 F=200 events=15000,100000 rate=200

Same settings + same seed = byte-identical files, so two runs of the delivery
system can be compared on exactly the same traffic.
"""

import csv
import heapq
import itertools
import os
import random
import sys
import uuid
from collections import Counter, defaultdict

# ---- endpoint layout: keep in step with the receiver ----------------------
# Receiver URLs are numbered in this order: all A, then B, C, D, E, F.
ENDPOINTS = {"A": 30, "B": 2, "C": 1, "D": 1, "E": 15, "F": 10}
NO_ENDPOINT_MERCHANTS = None  # None = 5 per A endpoint (brief: ~3,000 vs ~600)

# ---- lists to build --------------------------------------------------------
EVENT_COUNTS = [1500, 15000, 100000]
RATE = 5  # events/second the lists are timed for (the at_ms column)
SEED = 42

# ---- traffic model ---------------------------------------------------------
# Share of all events created by each merchant class. N = no endpoint of their own.
CLASS_SHARE = {"A": 0.70, "B": 0.05, "E": 0.10, "N": 0.15}
# Fraction of each class opted in to the fraud vendor (~22% of merchants, ~80% of volume).
FRAUD_OPTIN = {"A": 1.0, "B": 1.0, "E": 0.5, "N": 1 / 30}

PROFILE_NAME = {
    "A": "merchant_backend",
    "B": "erp_connector",
    "C": "fraud_vendor",
    "D": "analytics_pipeline",
    "E": "shared_hosting",
    "F": "ops_notifier",
}

ALL_TYPES = [
    "payment.authorized", "payment.captured", "payment.failed",
    "refund.created", "refund.settled",
    "payout.paid",
    "dispute.opened", "dispute.won", "dispute.lost",
    "subscription.renewed", "subscription.cancelled",
]
SUBSCRIBES = {
    "A": ALL_TYPES,
    "B": ALL_TYPES,
    "C": ["payment.authorized", "dispute.opened"],
    "D": ALL_TYPES,
    "E": ALL_TYPES,
    "F": ["payment.failed", "dispute.opened"],
}

# How often each kind of resource is started (per 100 payments).
NEW_RESOURCE = {"payment": 100, "subscription": 8, "payout": 0.025}
P_FAILED = 0.12       # payment declined outright: payment.failed, nothing else
P_CAPTURE = 0.98      # authorized payments that get captured
P_REFUND = 0.03       # captured payments later refunded
P_DISPUTE = 0.005     # captured payments later disputed
P_DISPUTE_WON = 0.30  # disputes the merchant wins
P_CANCEL = 0.05       # renewals followed by a cancellation

# Mean gap in seconds before a resource's next event. Real gaps are hours or
# days; these are compressed so lifecycles complete inside a test run.
DELAY_S = {
    "payment.captured": 20,
    "refund.created": 90,
    "refund.settled": 60,
    "dispute.opened": 120,
    "dispute.closed": 90,
    "subscription.cancelled": 60,
}

# What must already have happened to a resource before each event type.
REQUIRES = {
    "payment.captured": "payment.authorized",
    "refund.created": "payment.captured",
    "refund.settled": "refund.created",
    "dispute.opened": "payment.captured",
    "dispute.won": "dispute.opened",
    "dispute.lost": "dispute.opened",
    "subscription.cancelled": "subscription.renewed",
}


def parse_args(argv):
    for arg in argv:
        key, _, val = arg.partition("=")
        if key in ENDPOINTS:
            ENDPOINTS[key] = int(val)
        elif key == "N":
            globals()["NO_ENDPOINT_MERCHANTS"] = int(val)
        elif key == "events":
            globals()["EVENT_COUNTS"] = [int(v) for v in val.split(",")]
        elif key == "rate":
            globals()["RATE"] = float(val)
        elif key == "seed":
            globals()["SEED"] = int(val)
        else:
            sys.exit(f"unknown setting {arg!r}; use A..F, N, events, rate or seed")


# ---- the world: endpoints, merchants, subscriptions ------------------------

def build_world():
    rng = random.Random(f"world-{SEED}")

    endpoints = []
    by_profile = defaultdict(list)
    for p in "ABCDEF":
        for _ in range(ENDPOINTS[p]):
            idx = len(endpoints)
            endpoints.append({"endpoint": idx, "profile": p, "owner": ""})
            by_profile[p].append(idx)

    n_none = NO_ENDPOINT_MERCHANTS if NO_ENDPOINT_MERCHANTS is not None else 5 * ENDPOINTS["A"]
    merchants = []
    by_class = defaultdict(list)
    for cls in "ABE":
        for i, ep in enumerate(by_profile[cls]):
            m = new_merchant(f"{cls}{i:04d}", cls)
            m["own"] = ep
            endpoints[ep]["owner"] = m["id"]
            merchants.append(m)
            by_class[cls].append(m)
    for i in range(n_none):
        m = new_merchant(f"N{i:04d}", "N")
        merchants.append(m)
        by_class["N"].append(m)

    # Event share: split each class's share evenly across its merchants,
    # then renormalise so empty classes don't leave a hole.
    active = {c: s for c, s in CLASS_SHARE.items() if by_class[c]}
    total = sum(active.values())
    for c, share in active.items():
        for m in by_class[c]:
            m["weight"] = share / total / len(by_class[c])

    # Fraud opt-in, spread round-robin over the C endpoints.
    opted = []
    for c in "ABEN":
        members = by_class[c][:]
        rng.shuffle(members)
        opted += members[: round(len(members) * FRAUD_OPTIN[c])]
    opted.sort(key=lambda m: m["id"])
    if by_profile["C"]:
        for i, m in enumerate(opted):
            m["fraud"] = by_profile["C"][i % len(by_profile["C"])]

    # Every merchant reports to analytics, round-robin over the D endpoints.
    if by_profile["D"]:
        for i, m in enumerate(merchants):
            m["analytics"] = by_profile["D"][i % len(by_profile["D"])]

    # Ops notifiers are add-ons for the bigger merchants: ERP first, then backends.
    # More notifiers than candidates wraps round (Slack + PagerDuty on one merchant).
    candidates = by_class["B"] + by_class["A"]
    for i, ep in enumerate(by_profile["F"]):
        if not candidates:
            break
        m = candidates[i % len(candidates)]
        m["notifiers"].append(ep)
        endpoints[ep]["owner"] = m["id"]

    # Routing table.
    subs = []
    for m in merchants:
        if m["own"] is not None:
            subs.append((m["id"], m["own"], SUBSCRIBES[m["cls"]]))
        for ep in m["notifiers"]:
            subs.append((m["id"], ep, SUBSCRIBES["F"]))
        if m["fraud"] is not None:
            subs.append((m["id"], m["fraud"], SUBSCRIBES["C"]))
        if m["analytics"] is not None:
            subs.append((m["id"], m["analytics"], SUBSCRIBES["D"]))

    routes = defaultdict(list)
    for mid, ep, types in subs:
        routes[mid].append((ep, frozenset(types)))

    return endpoints, merchants, subs, routes


def new_merchant(mid, cls):
    return {"id": mid, "cls": cls, "weight": 0.0, "own": None,
            "notifiers": [], "fraud": None, "analytics": None}


# ---- the clock: one event per tick, lifecycles interleaved -----------------

def simulate(merchants, routes, n, rate):
    rng = random.Random(f"events-{SEED}-{n}-{rate}")
    pool = [m for m in merchants if m["weight"] > 0]
    cum = list(itertools.accumulate(m["weight"] for m in pool))
    kinds = list(NEW_RESOURCE)
    kind_cum = list(itertools.accumulate(NEW_RESOURCE.values()))
    serial = Counter()
    pending = []  # heap of (due_seconds, tiebreak, merchant_id, resource_id, event_type)
    tiebreak = itertools.count()
    rows = []

    def gap(kind):
        return max(1.0, rng.expovariate(1 / DELAY_S[kind]))

    def schedule(now, mid, rid, etype):
        nxt = []
        if etype == "payment.authorized" and rng.random() < P_CAPTURE:
            nxt.append(("payment.captured", gap("payment.captured")))
        elif etype == "payment.captured":
            if rng.random() < P_REFUND:
                nxt.append(("refund.created", gap("refund.created")))
            if rng.random() < P_DISPUTE:
                nxt.append(("dispute.opened", gap("dispute.opened")))
        elif etype == "refund.created":
            nxt.append(("refund.settled", gap("refund.settled")))
        elif etype == "dispute.opened":
            outcome = "dispute.won" if rng.random() < P_DISPUTE_WON else "dispute.lost"
            nxt.append((outcome, gap("dispute.closed")))
        elif etype == "subscription.renewed" and rng.random() < P_CANCEL:
            nxt.append(("subscription.cancelled", gap("subscription.cancelled")))
        for t, d in nxt:
            heapq.heappush(pending, (now + d, next(tiebreak), mid, rid, t))

    for k in range(n):
        now = k / rate
        remaining = n - k
        draining = remaining <= len(pending)
        if pending and (draining or pending[0][0] <= now):
            _, _, mid, rid, etype = heapq.heappop(pending)
        else:
            mid = rng.choices(pool, cum_weights=cum)[0]["id"]
            kind = rng.choices(kinds, cum_weights=kind_cum)[0]
            serial[kind] += 1
            if kind == "payment":
                rid = f"pay_{serial[kind]:07d}"
                etype = "payment.failed" if rng.random() < P_FAILED else "payment.authorized"
            elif kind == "subscription":
                rid = f"sub_{serial[kind]:07d}"
                etype = "subscription.renewed"
            else:
                rid = f"po_{serial[kind]:07d}"
                etype = "payout.paid"
        # Near the end, emit what is already scheduled but start nothing new,
        # so the list stops on whole (or cleanly truncated) lifecycles.
        if not draining:
            schedule(now, mid, rid, etype)
        eps = [ep for ep, types in routes[mid] if etype in types]
        rows.append({
            "seq": k,
            "at_ms": round(now * 1000),
            "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            "merchant_id": mid,
            "resource_id": rid,
            "event_type": etype,
            "endpoints": "|".join(str(e) for e in sorted(eps)),
        })
    return rows


def check_order(rows):
    seen = defaultdict(set)
    for r in rows:
        need = REQUIRES.get(r["event_type"])
        if need and need not in seen[r["resource_id"]]:
            sys.exit(f"ordering bug: {r['event_type']} before {need} on {r['resource_id']}")
        seen[r["resource_id"]].add(r["event_type"])


# ---- output ----------------------------------------------------------------

def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def summarise(rows, endpoints, rate):
    n = len(rows)
    secs = n / rate
    mix = Counter(r["event_type"] for r in rows)
    per_ep = Counter()
    per_sec = defaultdict(Counter)
    for r in rows:
        if r["endpoints"]:
            for e in r["endpoints"].split("|"):
                e = int(e)
                per_ep[e] += 1
                per_sec[e][r["at_ms"] // 1000] += 1
    deliveries = sum(per_ep.values())
    resources = Counter(r["resource_id"].split("_")[0] for r in {r["resource_id"]: r for r in rows}.values())

    out = [f"### {n:,} events at {rate:g}/s ({secs / 60:.1f} min of generation)", ""]
    out.append(f"Deliveries: **{deliveries:,}**, fan-out **{deliveries / n:.2f}** per event (brief: 2.3). "
               f"Resources started: {resources['pay']:,} payments, {resources['sub']:,} subscriptions, "
               f"{resources['po']:,} payouts.")
    out += ["", "| Event type | Count | Share |", "|---|---|---|"]
    for t in ALL_TYPES:
        if mix[t]:
            out.append(f"| `{t}` | {mix[t]:,} | {mix[t] / n * 100:.2f}% |")

    out += ["", "| Profile | Endpoints | Deliveries | Share | Mean per endpoint | Busiest endpoint | Busiest single second |",
            "|---|---|---|---|---|---|---|"]
    for p in "ABCDEF":
        eps = [e["endpoint"] for e in endpoints if e["profile"] == p]
        if not eps:
            continue
        d = sum(per_ep[e] for e in eps)
        busiest = max(eps, key=lambda e: per_ep[e])
        peak = max((max(per_sec[e].values()) for e in eps if per_sec[e]), default=0)
        out.append(f"| {p} {PROFILE_NAME[p]} | {len(eps)} | {d:,} | {d / max(deliveries, 1) * 100:.1f}% | "
                   f"{d / len(eps) / secs:.3f}/s | /{busiest}: {per_ep[busiest] / secs:.3f}/s | {peak} |")
    out.append("")
    return "\n".join(out)


def main():
    parse_args(sys.argv[1:])
    endpoints, merchants, subs, routes = build_world()
    n_none = sum(1 for m in merchants if m["cls"] == "N")
    name = "_".join(f"{p}{ENDPOINTS[p]}" for p in "ABCDEF") + f"_N{n_none}"
    folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "traces", name)
    os.makedirs(folder, exist_ok=True)

    write_csv(os.path.join(folder, "endpoints.csv"),
              ["endpoint", "url_path", "profile", "profile_name", "owner_merchant", "event_types"],
              [[e["endpoint"], f"/{e['endpoint']}", e["profile"], PROFILE_NAME[e["profile"]],
                e["owner"], "|".join(SUBSCRIBES[e["profile"]])] for e in endpoints])
    write_csv(os.path.join(folder, "merchants.csv"),
              ["merchant_id", "class", "event_share", "own_endpoint", "notifier_endpoints",
               "fraud_endpoint", "analytics_endpoint"],
              [[m["id"], m["cls"], f"{m['weight']:.6f}", "" if m["own"] is None else m["own"],
                "|".join(map(str, m["notifiers"])), "" if m["fraud"] is None else m["fraud"],
                "" if m["analytics"] is None else m["analytics"]] for m in merchants])
    write_csv(os.path.join(folder, "subscriptions.csv"),
              ["merchant_id", "endpoint", "event_types"],
              [[mid, ep, "|".join(types)] for mid, ep, types in subs])

    total_eps = len(endpoints)
    counts = ", ".join(f"{p}={ENDPOINTS[p]}" for p in "ABCDEF")
    header = [
        f"# Trace set {name}",
        "",
        f"Seed {SEED}. Set the receiver's profile counts to **{counts}** and "
        f"`Numendpoints` in endpoint.go to **{total_eps}**. Endpoint `n` in these files is receiver URL `/n`.",
        "",
        f"{len(merchants):,} merchants: " + ", ".join(
            f"{sum(1 for m in merchants if m['cls'] == c)} {c}" for c in "ABEN") +
        f". Fraud opt-in: {sum(1 for m in merchants if m['fraud'] is not None)} merchants, "
        f"{sum(m['weight'] for m in merchants if m['fraud'] is not None) * 100:.0f}% of event volume.",
        "",
    ]
    for n in EVENT_COUNTS:
        rows = simulate(merchants, routes, n, RATE)
        check_order(rows)
        fname = f"events_{n}_{RATE:g}ps.csv"
        write_csv(os.path.join(folder, fname),
                  ["seq", "at_ms", "event_id", "merchant_id", "resource_id", "event_type", "endpoints"],
                  [[r[k] for k in ("seq", "at_ms", "event_id", "merchant_id", "resource_id",
                                   "event_type", "endpoints")] for r in rows])
        with open(os.path.join(folder, f"summary_{n}_{RATE:g}ps.md"), "w", encoding="utf-8") as f:
            f.write("\n".join(header + [summarise(rows, endpoints, RATE)]))
        print(f"wrote {fname}")
    print(f"\n{folder}")
    print(f"receiver counts: {counts}   endpoint.go Numendpoints: {total_eps}")


if __name__ == "__main__":
    main()
