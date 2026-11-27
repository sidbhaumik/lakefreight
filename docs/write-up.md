# Row counts are not proof: what live testing taught me about moving data out of Snowflake

*Siddhartha Bhaumik · [lakefreight on GitHub](https://github.com/sidbhaumik/lakefreight)*

The first time lakefreight compared the values in its files with the values in Snowflake, 17 of 20 chunks failed.

Chunks just like them had passed every earlier run. The row counts matched exactly, the files were where they should be, and the Delta tables read back cleanly in Databricks. Every check I had said the data had arrived. It hadn't, quite: every timestamp had lost its last few digits on the way out, and nothing I was measuring could see it.

That moment sums up what building lakefreight taught me. lakefreight is an open-source tool that bulk-moves Snowflake tables into Parquet files on Amazon S3, then registers them as Delta Lake or Apache Iceberg tables that Databricks, Snowflake, Redshift or BigQuery can read. The moving part is not hard. Snowflake's `COPY INTO` will write Parquet straight to S3 in parallel. The hard part is **proving** it worked: that every row arrived, every value is unchanged, personal data stayed in its lane, and the target can read the result.

The project's habit is that no feature counts as done until it has run against real services: a Snowflake account, an AWS account and a Databricks workspace. Here are six problems I would never have found otherwise.

## 1. Matching row counts hid lost data

The standard check for a data migration is the row count: count the source, count what landed, compare. lakefreight did that from the start, for every chunk of every table, and refused to release a chunk until they matched.

But a row count passes when a column is cast wrong, when a join attaches the wrong value, or when precision quietly disappears. So in 0.8 I added a value fingerprint. Snowflake's `HASH_AGG` is computed over the source rows, using exactly the expressions the unload uses. It is computed again over the files just written, read back through the same stage. The chunk passes only if the two fingerprints are equal.

Getting them to agree on correct data meant modelling exactly what Snowflake's Parquet writer does, and that is where it got interesting:

- **Snowflake writes every Parquet timestamp in milliseconds**, whatever the column's precision and whatever you cast it to. I tried `TIMESTAMP_NTZ(6)` and switched off the physical type optimisation. Nothing kept the microseconds.
- **Before 1970 it rounds the wrong way.** The writer truncates toward zero on the epoch, so `1969-12-31 23:59:59.9995` becomes `1970-01-01 00:00:00.000`. That is a *later* instant, not an earlier one. My first fingerprint truncated toward negative infinity and disagreed on exactly those rows.

Once the fingerprint modelled the writer exactly, it agreed on all 17 column types I tested, from `BINARY` to `GEOGRAPHY`. Then it failed every chunk holding the demo data's sub-millisecond timestamps, the same kind of data that every earlier run had reconciled. Now lakefreight counts the values that would lose digits, names the columns, and fails the chunk unless you explicitly accept the loss. Silent precision loss became a visible decision.

It also catches what it should. In the live test, changing one email in the source failed the chunk. So did swapping two names between rows, which leaves every count and every set of values the same. So did an unload deliberately altered to write `UPPER(EMAIL)`.

**Lesson:** a check is only as good as what it can tell apart. Ask what wrong answer would still pass it.

## 2. The security control that wasn't there

lakefreight sorts tables into three sensitivity zones: restricted, personal data, and internal. Each zone has its own Snowflake role, stage, S3 prefix and KMS key. The point is that isolation is enforced by privileges, not by the code taking the right branch. If routing ever goes wrong, the unload should fail with a permission error, not leak.

On the first full run in AWS, every file landed in the right place and was encrypted with the right key. Everything passed. Then I tested the negative: could a session opened with the *internal* role list the *personal-data* stage?

It could. By default Snowflake activates every role granted to a user as a secondary role (`DEFAULT_SECONDARY_ROLES = ('ALL')`). The pipeline's service user holds all three zone roles, so every session quietly had all three. Routing was correct, so nothing failed, but the second line of defence I had designed did not exist.

The fix has two parts. Terraform now sets `default_secondary_roles_option = "NONE"` on the service user, and every connection runs `USE SECONDARY ROLES NONE`, which protects setups that don't use the Terraform. After the fix, the internal role could see only its own stage, and the whole pipeline reran with every item reconciling on the first attempt. That rerun also proved each role held every grant it needed on its own. It shipped as a security release, 0.5.1.

**Lesson:** a security boundary you have only tested on the happy path is a hope, not a control. Test that the wrong thing fails.

## 3. My test doubles agreed with my assumptions

lakefreight's offline tests are thorough, and two bugs sailed straight through them.

- **The personal-data detector matched almost nothing.** Discovery flags columns by name with regular expressions, such as `PASSPORT`. The tests ran the generated SQL in DuckDB, where `REGEXP_LIKE` matches anywhere in the string. Snowflake's matches only the whole string, so `PASSPORT` missed `PASSPORT_NUMBER`. In a tool whose job is to keep personal data out of the open zone, that matters.
- **The reconcile Lambda would have crashed.** With Snowflake's Python connector 5.x, a cursor fetched by query ID has no column descriptions until you read its rows. My fake cursor had them all along.

Both are fixed, and the fakes now behave like the real connector, including its awkward parts.

**Lesson:** a mock encodes your understanding of a system, so it can't find where that understanding is wrong. Only the real system can.

## 4. Tables that change while you copy them

Big tables are split into chunks, each its own `COPY`. If the table is still being written, which it usually is in a real migration, each chunk sees a different moment, and the source count taken during reconciliation sees yet another.

0.9 pins every query for such a table to one Snowflake time-travel snapshot: the profile, every chunk's unload, every reconciliation query, and retries too. To prove it worked, I ran a controlled experiment. A 2-million-row table was extracted through the real Lambda handlers while a second connection kept updating, deleting and inserting rows across its whole key range.

| Run | Writes during the run | Result |
|---|---|---|
| Pinned to a snapshot | 42 | Every chunk reconciled: exactly 2,000,000 rows, the table at the snapshot, while the live table drifted to 1,999,384 |
| Not pinned (control) | 60 | Failed: `row count mismatch: unloaded 1048398, source 1048440` |

The control run matters as much as the pinned one. Without it, "it passed" could mean the writes just didn't collide.

Building it also surfaced a quieter bug. A `SNAPSHOT_TS` column already existed, but the submit step overwrote it with the submit time, so the same field meant two different things. Now it means one thing: the instant every query reads at. Time travel only reaches back as far as each table's retention, so the planner also refuses a table that could age out mid-extract, before it plans anything.

**Lesson:** to prove a fix, show the failure without it. And a field with two meanings is a bug waiting for a reader.

## 5. Splitting tables without losing rows

Chunking looks simple: pick a date column, cut it into months. Real tables make that wrong in several ways at once.

- **Volume isn't even.** Recent months dwarf old ones, so lakefreight packs months to a target size, and splits a month into day ranges when one month alone is too big. One test table had 80% of its rows in a single March.
- **Closed ranges lose rows.** A predicate built from the profiled minimum and maximum misses rows that arrive later, and no range ever matches NULL. So the first chunk has no lower bound, the last has no upper bound, and there is always a NULL chunk.
- **A chunk that doesn't prune scans the whole table.** Chunking on an unclustered column turns 20 chunks into 20 full scans. lakefreight now reads each table's clustering key and chunks on the column it leads with. Tables with no date column are chunked by integer key ranges instead.

On a 10-million-row table with negative keys, gaps and NULL keys, every row fell in exactly one chunk, and every chunk query scanned only part of the table.

**Lesson:** for anything that partitions data, the property to prove is "every row, exactly once", including the rows you didn't expect.

## 6. A pipeline that reported success when everything failed

On the first AWS run, every unload failed: the account was on AWS's free plan, whose organization-level policies, as far as I could tell, block Snowflake from assuming a role in it. That was an account problem, and the next run used a standard account. But it exposed something worse in my own code. Step Functions marked every lane as **succeeded**.

The Distributed Map was set to tolerate failed items, so each item would get its own attempt, and that also made the execution succeed. A new step now counts the run's items by status and fails the lane with `LaneHadFailures` if any did not reconcile. On the next run I broke a chunk on purpose, with three tokens missing from the detokenization cross-reference. The lane failed, the chunk got no manifest, registration refused the incomplete table, and the retry fixed only that one chunk.

**Lesson:** the most dangerous failure is the one reported as success. Break things on purpose and watch what the system says.

## What I'd tell myself at the start

- **Write down what each claim rests on.** The claims in lakefreight's README link to [live validation](https://github.com/sidbhaumik/lakefreight/blob/main/docs/live-validation.md): what was run, the numbers, and what it found, bugs included. Writing it down forced me to run what I'd only assumed.
- **Design for the failure you can't see.** Fingerprints, manifests written last as commit markers, a mandatory NULL chunk, and zones that fail closed all exist because the worst outcome of a migration is not an error. It is a confident, wrong success.
- **Be honest about scope.** lakefreight hasn't run a production-scale migration yet, and its [known gaps](https://github.com/sidbhaumik/lakefreight/blob/main/docs/design-decisions.md#known-gaps) say so, along with what it leaves out by design.

lakefreight 1.0 is on [PyPI](https://pypi.org/project/lakefreight/) and [GitHub](https://github.com/sidbhaumik/lakefreight), MIT-licensed. You can try the planner in two minutes, with no Snowflake or AWS account, and the [design decisions](https://github.com/sidbhaumik/lakefreight/blob/main/docs/design-decisions.md) explain each choice and what was rejected. I'd like to hear from anyone who has fought the same problems.
