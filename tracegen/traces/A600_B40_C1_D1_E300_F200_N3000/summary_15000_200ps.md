# Trace set A600_B40_C1_D1_E300_F200_N3000

Seed 42. Set the receiver's profile counts to **A=600, B=40, C=1, D=1, E=300, F=200** and `Numendpoints` in endpoint.go to **1142**. Endpoint `n` in these files is receiver URL `/n`.

3,940 merchants: 600 A, 40 B, 300 E, 3000 N. Fraud opt-in: 890 merchants, 80% of event volume.

### 15,000 events at 200/s (1.2 min of generation)

Deliveries: **33,522**, fan-out **2.23** per event (brief: 2.3). Resources started: 7,616 payments, 596 subscriptions, 3 payouts.

| Event type | Count | Share |
|---|---|---|
| `payment.authorized` | 6,695 | 44.63% |
| `payment.captured` | 6,542 | 43.61% |
| `payment.failed` | 921 | 6.14% |
| `refund.created` | 147 | 0.98% |
| `refund.settled` | 38 | 0.25% |
| `payout.paid` | 3 | 0.02% |
| `dispute.opened` | 19 | 0.13% |
| `dispute.won` | 2 | 0.01% |
| `dispute.lost` | 2 | 0.01% |
| `subscription.renewed` | 596 | 3.97% |
| `subscription.cancelled` | 35 | 0.23% |

| Profile | Endpoints | Deliveries | Share | Mean per endpoint | Busiest endpoint | Busiest single second |
|---|---|---|---|---|---|---|
| A merchant_backend | 600 | 10,501 | 31.3% | 0.233/s | /36: 0.493/s | 4 |
| B erp_connector | 40 | 844 | 2.5% | 0.281/s | /621: 0.440/s | 3 |
| C fraud_vendor | 1 | 5,456 | 16.3% | 72.747/s | /640: 72.747/s | 128 |
| D analytics_pipeline | 1 | 15,000 | 44.7% | 200.000/s | /641: 200.000/s | 200 |
| E shared_hosting | 300 | 1,498 | 4.5% | 0.067/s | /728: 0.213/s | 2 |
| F ops_notifier | 200 | 223 | 0.7% | 0.015/s | /961: 0.067/s | 2 |
