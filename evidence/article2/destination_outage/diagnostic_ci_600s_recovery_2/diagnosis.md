# CI diagnostic: second 600-second checkpoint deadline

Run [39484897847, repetition 2](https://github.com/hachej/boring-cdc/actions/runs/39484897847) executed at exact Git head `c39e12b889b95d7dc679af6c2eaf96814f10e08a`. The packet is retained unchanged and remains **failed**: the destination checkpoint was seven journal events behind when the recovery allowance ended. Peak retained WAL was 287,760 bytes and peak destination lag 74 events. All 57 distinct business event IDs were present, with zero duplicates and an exact independent PostgreSQL keyed oracle match. No operator step was taken.

The two repetitions show the checkpoint converging more slowly than the 600-second allowance while periodic control events continue. The next run extends the observation allowance to 1,200 seconds; the outage, workload, sampling and assertions remain unchanged.
