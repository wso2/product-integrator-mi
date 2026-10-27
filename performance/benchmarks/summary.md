# WSO2 Micro Integrator 4.7.0 Performance Test Results

During each release, we execute various automated performance test scenarios and publish the results.

| Test Scenarios | Description |
| --- | --- |
| Direct Proxy | Passthrough proxy service |
| CBR Proxy | Routing the message based on the content of the message body |
| XSLT Proxy | Having XSLT transformations in request and response paths |
| Direct API | Passthrough API service |

Our test client is [Apache JMeter](https://jmeter.apache.org/index.html). We test each scenario for a fixed duration of
time. We split the test results into warmup and measurement parts and use the measurement part to compute the
performance metrics.

Test scenarios use a [Netty](https://netty.io/) based back-end service which echoes back any request
posted to it after a specified period of time.

We run the performance tests under different numbers of concurrent users, message sizes (payloads) and back-end service
delays.

The main performance metrics:

1. **Throughput**: The number of requests that the WSO2 Micro Integrator 4.7.0 processes during a specific time interval (e.g. per second).
2. **Response Time**: The end-to-end latency for an operation of invoking a service in WSO2 Micro Integrator 4.7.0 . The complete distribution of response times was recorded.

In addition to the above metrics, we measure the load average and several memory-related metrics.

The following are the test parameters.

| Test Parameter | Description | Values |
| --- | --- | --- |
| Scenario Name | The name of the test scenario. | Refer to the above table. |
| Heap Size | The amount of memory allocated to the application | 2G |
| Concurrent Users | The number of users accessing the application at the same time. | 100, 200, 500, 1000 |
| Message Size (Bytes) | The request payload size in Bytes. | 500, 1000, 10000, 100000 |
| Back-end Delay (ms) | The delay added by the Back-end service. | 0 |

The duration of each test is **300 seconds**. The warm-up period is **120 seconds**.
The measurement results are collected after the warm-up period.

The performance tests were executed on 1 AWS CloudFormation stack.


System information for WSO2 Micro Integrator 4.7.0 in 1st AWS CloudFormation stack.

| Class | Subclass | Description | Value |
| --- | --- | --- | --- |
| AWS | EC2 | AMI-ID | ami-0d6dd9e8265d48906 |
| AWS | EC2 | Instance Type | c5.large |
| System | Processor | CPU(s) | 2 |
| System | Processor | Thread(s) per core | 2 |
| System | Processor | Core(s) per socket | 1 |
| System | Processor | Socket(s) | 1 |
| System | Processor | Model name | Intel(R) Xeon(R) Platinum 8124M CPU @ 3.00GHz |
| System | Memory | BIOS | 64 KiB |
| System | Memory | System Memory | 4 GiB |
| System | Storage | Block Device: nvme0n1 | 8G |
| Operating System | Distribution | Release | Ubuntu 24.04.5 LTS |
| Operating System | Distribution | Kernel | Linux ip-10-0-0-149 7.0.0-1014-aws #14~24.04.1-Ubuntu SMP PREEMPT Fri Sep 18 21:24:59 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux |


The following are the measurements collected from each performance test conducted for a given combination of
test parameters.

| Measurement | Description |
| --- | --- |
| Error % | Percentage of requests with errors |
| Average Response Time (ms) | The average response time of a set of results |
| Standard Deviation of Response Time (ms) | The “Standard Deviation” of the response time. |
| 99th Percentile of Response Time (ms) | 99% of the requests took no more than this time. The remaining samples took at least as long as this |
| Throughput (Requests/sec) | The throughput measured in requests per second. |
| Average Memory Footprint After Full GC (M) | The average memory consumed by the application after a full garbage collection event. |

The following is the summary of performance test results collected for the measurement period.

|  Scenario Name | Heap Size | Concurrent Users | Message Size (Bytes) | Back-end Service Delay (ms) | Error % | Throughput (Requests/sec) | Average Response Time (ms) | Standard Deviation of Response Time (ms) | 99th Percentile of Response Time (ms) | WSO2 Micro Integrator 4.7.0 GC Throughput (%) | Average WSO2 Micro Integrator 4.7.0 Memory Footprint After Full GC (M) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
|  CBR Proxy | 2G | 100 | 500 | 0 | 0.00 | 5227.55 | 19.06 | 21.25 | 90.00 | 99.32 |  |
|  CBR Proxy | 2G | 100 | 1000 | 0 | 0.00 | 4566.26 | 21.83 | 25.64 | 106.00 | 99.31 |  |
|  CBR Proxy | 2G | 100 | 10000 | 0 | 0.00 | 1405.19 | 71.05 | 59.72 | 236.00 | 98.87 |  |
|  CBR Proxy | 2G | 100 | 100000 | 0 | 0.00 | 99.85 | 1000.17 | 263.69 | 1767.00 | 78.96 | 246.059 |
|  CBR Proxy | 2G | 200 | 500 | 0 | 0.00 | 5090.30 | 39.22 | 59.11 | 222.00 | 99.2 |  |
|  CBR Proxy | 2G | 200 | 1000 | 0 | 0.00 | 4512.72 | 44.24 | 67.52 | 253.00 | 99.11 |  |
|  CBR Proxy | 2G | 200 | 10000 | 0 | 0.00 | 1393.21 | 143.43 | 105.14 | 519.00 | 98.42 |  |
|  CBR Proxy | 2G | 200 | 100000 | 0 | 0.00 | 80.76 | 2453.92 | 684.40 | 4079.00 | 71.04 | 398 |
|  CBR Proxy | 2G | 500 | 500 | 0 | 0.00 | 5068.45 | 98.45 | 187.28 | 619.00 | 98.76 |  |
|  CBR Proxy | 2G | 500 | 1000 | 0 | 0.00 | 4412.89 | 113.09 | 215.59 | 703.00 | 98.75 |  |
|  CBR Proxy | 2G | 500 | 10000 | 0 | 0.00 | 1320.78 | 378.19 | 223.87 | 1287.00 | 96.6 |  |
|  CBR Proxy | 2G | 500 | 100000 | 0 | 0.00 | 61.08 | 7958.52 | 2636.22 | 13183.00 | 63.52 | 814 |
|  CBR Proxy | 2G | 1000 | 500 | 0 | 0.00 | 4935.40 | 202.27 | 200.64 | 775.00 | 98.22 |  |
|  CBR Proxy | 2G | 1000 | 1000 | 0 | 0.00 | 4352.89 | 229.27 | 223.26 | 855.00 | 97.99 |  |
|  CBR Proxy | 2G | 1000 | 10000 | 0 | 0.00 | 903.05 | 1101.56 | 712.98 | 3791.00 | 82.49 | 307.294 |
|  CBR Proxy | 2G | 1000 | 100000 | 0 | 0.00 | 34.74 | 26500.62 | 7646.43 | 40703.00 | 49.64 | 1523.86 |
|  Direct API | 2G | 100 | 500 | 0 | 0.00 | 7858.45 | 12.66 | 7.71 | 35.00 | 99.43 |  |
|  Direct API | 2G | 100 | 1000 | 0 | 0.00 | 7703.44 | 12.92 | 7.63 | 35.00 | 99.43 |  |
|  Direct API | 2G | 100 | 10000 | 0 | 0.00 | 5697.15 | 17.48 | 10.68 | 47.00 | 99.53 |  |
|  Direct API | 2G | 100 | 100000 | 0 | 0.00 | 1734.12 | 57.54 | 12.18 | 90.00 | 99.66 |  |
|  Direct API | 2G | 200 | 500 | 0 | 0.00 | 7876.66 | 25.32 | 14.07 | 70.00 | 99.36 |  |
|  Direct API | 2G | 200 | 1000 | 0 | 0.00 | 7643.56 | 26.10 | 14.94 | 75.00 | 99.38 |  |
|  Direct API | 2G | 200 | 10000 | 0 | 0.00 | 5606.70 | 35.58 | 17.98 | 92.00 | 99.45 |  |
|  Direct API | 2G | 200 | 100000 | 0 | 0.00 | 1698.14 | 117.61 | 21.92 | 177.00 | 99.65 |  |
|  Direct API | 2G | 500 | 500 | 0 | 0.00 | 7325.27 | 68.14 | 32.53 | 173.00 | 99.18 |  |
|  Direct API | 2G | 500 | 1000 | 0 | 0.00 | 7431.56 | 67.16 | 31.26 | 162.00 | 99.2 |  |
|  Direct API | 2G | 500 | 10000 | 0 | 0.00 | 5822.75 | 85.74 | 36.05 | 195.00 | 99.35 |  |
|  Direct API | 2G | 500 | 100000 | 0 | 0.00 | 1595.77 | 313.11 | 43.71 | 427.00 | 99.57 |  |
|  Direct API | 2G | 1000 | 500 | 0 | 0.00 | 7425.39 | 134.47 | 53.99 | 299.00 | 98.91 |  |
|  Direct API | 2G | 1000 | 1000 | 0 | 0.00 | 7230.19 | 138.12 | 54.98 | 305.00 | 98.91 |  |
|  Direct API | 2G | 1000 | 10000 | 0 | 0.00 | 5837.12 | 171.08 | 62.57 | 349.00 | 99.15 |  |
|  Direct API | 2G | 1000 | 100000 | 0 | 0.00 | 1570.47 | 635.41 | 81.98 | 851.00 | 99.53 |  |
|  Direct Proxy | 2G | 100 | 500 | 0 | 0.00 | 8910.87 | 11.16 | 6.48 | 29.00 | 99.39 |  |
|  Direct Proxy | 2G | 100 | 1000 | 0 | 0.00 | 8718.30 | 11.40 | 6.80 | 31.00 | 99.43 |  |
|  Direct Proxy | 2G | 100 | 10000 | 0 | 0.00 | 6324.79 | 15.74 | 9.59 | 42.00 | 99.5 |  |
|  Direct Proxy | 2G | 100 | 100000 | 0 | 0.00 | 1878.67 | 53.11 | 10.73 | 82.00 | 99.66 |  |
|  Direct Proxy | 2G | 200 | 500 | 0 | 0.00 | 8802.58 | 22.65 | 12.19 | 62.00 | 99.35 |  |
|  Direct Proxy | 2G | 200 | 1000 | 0 | 0.00 | 8627.51 | 23.11 | 12.77 | 63.00 | 99.38 |  |
|  Direct Proxy | 2G | 200 | 10000 | 0 | 0.00 | 6219.25 | 32.07 | 17.86 | 87.00 | 99.48 |  |
|  Direct Proxy | 2G | 200 | 100000 | 0 | 0.00 | 1781.35 | 112.09 | 19.98 | 166.00 | 99.62 |  |
|  Direct Proxy | 2G | 500 | 500 | 0 | 0.00 | 8169.94 | 61.10 | 28.79 | 153.00 | 99.19 |  |
|  Direct Proxy | 2G | 500 | 1000 | 0 | 0.00 | 8020.04 | 62.23 | 28.71 | 153.00 | 99.24 |  |
|  Direct Proxy | 2G | 500 | 10000 | 0 | 0.00 | 6264.61 | 79.68 | 33.86 | 181.00 | 99.35 |  |
|  Direct Proxy | 2G | 500 | 100000 | 0 | 0.00 | 1676.00 | 298.28 | 41.31 | 405.00 | 99.54 |  |
|  Direct Proxy | 2G | 1000 | 500 | 0 | 0.00 | 7937.56 | 125.79 | 48.48 | 269.00 | 98.94 |  |
|  Direct Proxy | 2G | 1000 | 1000 | 0 | 0.00 | 8091.25 | 123.39 | 48.38 | 267.00 | 98.89 |  |
|  Direct Proxy | 2G | 1000 | 10000 | 0 | 0.00 | 6185.44 | 161.46 | 58.38 | 325.00 | 99.08 |  |
|  Direct Proxy | 2G | 1000 | 100000 | 0 | 0.00 | 1616.43 | 617.68 | 82.13 | 839.00 | 99.52 |  |
|  XSLT Proxy | 2G | 100 | 500 | 0 | 0.00 | 2514.30 | 39.69 | 44.40 | 177.00 | 98.51 |  |
|  XSLT Proxy | 2G | 100 | 1000 | 0 | 0.00 | 1646.56 | 52.24 | 211.03 | 198.00 | 98.66 |  |
|  XSLT Proxy | 2G | 100 | 10000 | 0 | 0.00 | 357.69 | 279.25 | 229.08 | 1591.00 | 98.38 |  |
|  XSLT Proxy | 2G | 100 | 100000 | 0 | 0.00 | 27.40 | 3602.35 | 617.50 | 5311.00 | 87.21 | 257.538 |
|  XSLT Proxy | 2G | 200 | 500 | 0 | 0.00 | 2433.08 | 82.14 | 114.35 | 439.00 | 98.3 |  |
|  XSLT Proxy | 2G | 200 | 1000 | 0 | 0.00 | 1830.09 | 109.16 | 102.32 | 477.00 | 98.5 |  |
|  XSLT Proxy | 2G | 200 | 10000 | 0 | 0.00 | 346.36 | 576.02 | 419.34 | 2479.00 | 97.6 |  |
|  XSLT Proxy | 2G | 200 | 100000 | 0 | 0.00 | 24.93 | 7842.19 | 2019.34 | 13887.00 | 84.69 | 439.556 |
|  XSLT Proxy | 2G | 500 | 500 | 0 | 0.00 | 2320.24 | 215.10 | 317.15 | 1119.00 | 97.59 |  |
|  XSLT Proxy | 2G | 500 | 1000 | 0 | 0.00 | 1818.34 | 274.46 | 329.73 | 1423.00 | 97.73 |  |
|  XSLT Proxy | 2G | 500 | 10000 | 0 | 0.00 | 274.82 | 1806.47 | 987.42 | 5183.00 | 91.71 | 414.833 |
|  XSLT Proxy | 2G | 500 | 100000 | 0 | 0.00 | 22.12 | 20526.86 | 9419.77 | 46335.00 | 81.57 | 834.304 |
|  XSLT Proxy | 2G | 1000 | 500 | 0 | 0.00 | 1806.77 | 551.65 | 456.39 | 1975.00 | 92.17 | 357 |
|  XSLT Proxy | 2G | 1000 | 1000 | 0 | 0.00 | 1418.79 | 702.22 | 570.52 | 2527.00 | 91.88 | 327 |
|  XSLT Proxy | 2G | 1000 | 10000 | 0 | 0.00 | 259.84 | 3782.15 | 1736.09 | 8447.00 | 88.67 | 496.167 |
|  XSLT Proxy | 2G | 1000 | 100000 | 0 | 0.00 | 16.79 | 48431.93 | 11931.06 | 79359.00 | 74.11 | 1287.784 |
