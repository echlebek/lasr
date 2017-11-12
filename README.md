[![Build Status](https://api.travis-ci.org/echlebek/lasr.svg)](https://api.travis-ci.org/echlebek/lasr)

# lasr
A persistent message queue backed by BoltDB. This queue is useful when the producers and consumers can live in the same process.

Project goals
-------------
  * Data integrity over performance.
  * Simplicity over complexity.
  * Ease of use.
  * Minimal feature set.

Safety
------
lasr is designed to never lose information. When the Send method completes, messages have been safely written to disk. On Receive, messages are not deleted until Ack is called. Users should make sure they always respond to messages with Ack or Nack.

Misc
----
Dead-lettering is supported, but disabled by default.

Benchmarks
----------

On a late 2012 Macbook Pro,

`$ hey -m POST -D main.go -h2 -cpus 2 -n 20000 -c 10 http://localhost:8080`

```
Summary:
  Total:         11.4155 secs
  Slowest:       0.0342 secs
  Fastest:       0.0006 secs
  Average:       0.0057 secs
  Requests/sec:  1752.0004

Status code distribution:
  [200]    20000 responses

Response time histogram:
  0.001 [1]     |
  0.004 [308]   |∎
  0.007 [18044] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
  0.011 [1189]  |∎∎∎
  0.014 [357]   |∎
  0.017 [58]    |
  0.021 [19]    |
  0.024 [7]     |
  0.028 [2]     |
  0.031 [10]    |
  0.034 [5]     |

Latency distribution:
  10% in 0.0047 secs
  25% in 0.0050 secs
  50% in 0.0053 secs
  75% in 0.0058 secs
  90% in 0.0068 secs
  95% in 0.0089 secs
  99% in 0.0120 secs
```
