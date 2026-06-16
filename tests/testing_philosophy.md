# Testing Philosophy
 
This document explains what we test in this pipeline, what we do not, and
why. The goal is a small suite of tests that each protect something real.
A test that cannot fail for a meaningful reason is worse than no test: it
costs maintenance and gives false confidence.
 
## The four rules
 
### 1. Test business logic exhaustively
 
Any code that parses, cleans, transforms, or computes is where bugs hide,
so this is where coverage goes. Parsing functions, cleaning rules,
aggregation math, and quality check thresholds all get tested against real
inputs with known outputs.
 
Example from this repo: `clean_weather` is tested for temperature range
validation, single day gap interpolation, the `is_snowy` rule (precip over
0.5mm AND temp at or below 1.0C), and the year filter. The gold
aggregation tests construct a small known input and assert exact values
(three trips, total revenue exactly 46.0), so a broken aggregate shows up
as an obvious wrong number.
 
### 2. One contract test per external boundary
 
Where our code meets the outside world (the internet, S3, the Airflow
scheduler) we keep exactly one test proving the connection behaves. One,
not three. More tests on the same boundary add maintenance cost without
catching anything new.
 
Example from this repo: source availability is covered by a single test
that confirms a 404 returns False (the branch that drives skip this
month). Idempotency is covered by one skip when already exists test. The
DAG has one parses without error test.
 
### 3. Delete mock-echo tests
 
A mock is a stand in for something slow or external. Using one is fine. The
problem is a test that sets a mock to return a value and then only checks
it got that value back. Such a test exercises no code of ours. It cannot
fail, so it cannot warn us.
 
Example from this repo: a removed test set a fake S3 client to not raise an
error, then asserted the function returned True. It tested the fake, not
our logic. The same function is still covered for real by the idempotency
contract test, which runs actual branching.
 
### 4. Do not test other people's tools
 
We do not test that boto3 talks to S3, that requests handles HTTP, or that
Spark configures a session. Those libraries test themselves. Re-testing
them is like checking the wall outlet works before plugging in a lamp.
 
Example from this repo: a removed test checked that `requests.head`
returning an exception was handled. That is the HTTP library's behavior,
not ours.
 
## What we deliberately do not test, and why
 
**boto3 and S3 plumbing.** Covered by AWS and boto3. We test our threshold
logic (does the size check correctly sum file sizes and compare), not the
S3 call itself.
 
**Spark session configuration.** We test the transform logic that runs
inside a session, not that the session starts with the right config.
 
**The mechanics of mocks.** If an assertion only confirms a mock returned
its configured value, it gets deleted.
 
**Duplicate boundary cases.** Once one contract test proves a boundary
works, extra variations on the same boundary are removed.
 
**PySpark transforms run locally.** These tests require Spark, which is not
installed on every dev machine. They skip locally and run on CI, where
Spark and Java are present. A local skip is expected, not a failure.
 
## How to add a test
 
Before writing a test, ask which rule it serves. If it tests our logic
(rule 1) or proves a boundary once (rule 2), write it. If it only echoes a
mock (rule 3) or tests a library we do not own (rule 4), do not. Match the
existing style: class based grouping, a docstring stating the behavior
under test, and assertions on exact values where possible.