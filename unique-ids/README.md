## Unique Id Generation

```bash
#test params

maelstrom test -w unique-ids --bin ~/go/bin/maelstrom-unique-ids --time-limit 60 --rate 10000 --node-count 10 --nemesis partition --availability total
```
Executes the unique-ids workload
- for 30 seconds
- with upto 10k requests per second
- on 10 nodes
- with possible network partitions
- checks for total availability
also Analyses the ids generated to ensure they're unique.

Test output
```text
 {:perf {:latency-graph {:valid? true},
        :rate-graph {:valid? true},
        :valid? true},
 :timeline {:valid? true},
 :exceptions {:valid? true},
 :stats {:valid? true,
         :count 238845,
         :ok-count 238845,
         :fail-count 0,
         :info-count 0,
         :by-f {:generate {:valid? true,
                           :count 238845,
                           :ok-count 238845,
                           :fail-count 0,
                           :info-count 0}}},
 :availability {:valid? true, :ok-fraction 1.0},
 :net {:all {:send-count 477710,
             :recv-count 477710,
             :msg-count 477710,
             :msgs-per-op 2.0000837},
       :clients {:send-count 477710,
                 :recv-count 477710,
                 :msg-count 477710},
       :servers {:send-count 0,
                 :recv-count 0,
                 :msg-count 0,
                 :msgs-per-op 0.0},
       :valid? true},
 :workload {:valid? true,
            :attempted-count 238845,
            :acknowledged-count 238845,
            :duplicated-count 0,
            :duplicated {},
            :range ["12616_10006300_1894" "9132_999994800_23339"]},
 :valid? true}

Everything looks good!
```
Total 238,845 ops requests were sent and 238,845 unique ids were generated. yay!