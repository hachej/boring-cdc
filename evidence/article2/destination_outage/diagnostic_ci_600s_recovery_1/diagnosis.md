# CI diagnostic: checkpoint lag after 600-second allowance

Run [39484897847, repetition 1](https://github.com/hachej/boring-cdc/actions/runs/39484897847) executed at exact Git head `c39e12b889b95d7dc679af6c2eaf96814f10e08a`. The packet is retained unchanged and remains **failed**: the destination checkpoint was 12 journal events behind after the 600-second recovery allowance. No operator step was taken.

The failure is the recovery deadline, not business-event loss: peak retained WAL was 288,432 bytes, peak lag 78 events, the 57 distinct business IDs were present with zero duplicates, and the independent PostgreSQL keyed oracle matched. The checkpoint lag declined from 27 at elapsed 600 seconds to 12 at elapsed 815 seconds while periodic control events continued. The next run allows up to 1,200 seconds to observe whether the checkpoint itself reaches the journal head. The outage duration, 24-transaction workload, sampling and pass assertions remain unchanged.
