# Run reports

The runs referred to in the main README. Each folder holds the HTML report for that run (open `report.html` in a browser), and a CPU profile (`cpu.pprof`) where one was taken. Profiles open with `go tool pprof -top cpu.pprof` or `go tool pprof -http=:8081 cpu.pprof`.

Run numbers match the main README. Earlier runs (1–47) and the raw CSV logs behind each report aren't included; the CSV file names listed at the bottom of each report refer to those logs.

| Run | Date | Load | What changed | Headline |
|---|---|---|---|---|
| [48](run48/report.html) | 4 Oct 2026 | 750 events/s, 1,142 endpoints | Rate raised from 181/s, 40 fixed lanes per endpoint | Fraud vendor capped near 267 deliveries/s by its 40 lanes: 358 refused |
| [49](run49/report.html) | 5 Oct 2026 | 760 events/s | Lanes resize at runtime (+10 per resize) | Every resize waited for the logger's 1 s sleep: 6,164 refused |
| [50](run50/report.html) | 5 Oct 2026 | 780 events/s | Logger and retry helper wake on a resize | Pauses down to one request, nothing refused |
| [51](run51/report.html) | 6 Oct 2026 | 785 events/s | Every endpoint starts at 5 lanes instead of 40 | Goroutines −81%, memory −70%, everything delivered |
| [56](run56/report.html) | 6 Oct 2026 | 1,200 events/s, 250 s | Repeat of run 55 with a CPU profile | 666,977 deliveries, none refused; scheduler a third of CPU |
| [57](run57/report.html) | 6 Oct 2026 | 3,000 events/s, 100 s | Rate raised to the brief's peak | +10 lane growth too slow: 1,604 refused during the ramp |
| [59](run59/report.html) | 6 Oct 2026 | 3,000 events/s | Lanes double instead of +10 | Growth in 7 steps; 601 refused, all at resize moments |
| [60](run60/report.html) | 7 Oct 2026 | 200 events/s, 75 s | Current code (after cleanup) | Normal day: all 33,522 delivered |
| [61](run61/report.html) | 7 Oct 2026 | 3,000 events/s, 100 s | Current code (after cleanup) | Busiest day: 538 refused (resizes and one ~150 ms stall), 0 ordering violations |
