# Project sections (in build order)

- Synthetic data generator + Kafka producer — PaySim seed data, Faker-based anomaly injection, streaming into Kafka
- Spark Structured Streaming fraud detection — consume from Kafka, apply windowed rules, output flags
- Serving layer + raw lake — Postgres table for flagged transactions, MinIO/S3 for raw event storage
- Batch ingestion + Airflow DAG — load Kaggle loan/fraud datasets and CBSL exchange rates, orchestrate daily reconciliation
- dbt transformations — staging models, star schema (fact/dim tables), data quality tests
- BI dashboard — Superset/Metabase on top of the warehouse and serving layer
- Testing, README, and demo polish — unit tests, architecture diagram, GIF/screenshot, final documentation

## Section 1: Purpose and overview (in depth)

### Why this section exists
Every data engineering project needs a source of truth — something producing data for the rest of your pipeline to consume. In a real fintech company, that source would be an actual banking core system, a payment gateway, or a mobile money platform emitting transaction events. You obviously can't get access to that, so Section 1's job is to simulate that role convincingly enough that everything downstream (Spark, Airflow, dbt, dashboards) behaves exactly as it would in production.

This is not a throwaway step — it's arguably the most interview-relevant part of the whole project, because it's where you demonstrate you understand:
- How real event-driven systems are structured (a producer emitting discrete events, not a static file)
- How to reason about data realistically (transaction distributions, timing, fraud patterns) rather than just using random.randint()
- Why streaming architectures use a message broker (Kafka) as a buffer between the producer and consumers, instead of writing directly to a database

### What "purpose" this section serves in the bigger picture
Think of Section 1 as building the heartbeat of your system. Once it's running, it continuously pushes events into Kafka, and everything else you build in later sections — Spark streaming (Section 2), the raw lake (Section 3), even the batch reconciliation (Section 4) — depends on this heartbeat existing. If Section 1 is weak (e.g. purely random data with no realistic fraud signal), your fraud detection logic in Section 2 will have nothing meaningful to catch, and your whole demo will fall flat in an interview.

So the real purpose is twofold:
- Technical purpose: produce a Kafka topic (transactions.raw) continuously receiving JSON transaction events, at a controllable rate, so you can demo "real-time" processing live.
- Narrative purpose: give yourself a story to tell. "I used PaySim as a statistically realistic base, then injected known fraud patterns so I could prove my detection logic actually catches them" is a genuinely strong sentence in an interview — it shows you engineered the problem, not just the pipeline.

### Breaking down the two halves of Section 1

**Half A — The realistic base (PaySim)**
PaySim is a synthetic dataset generator originally built to mimic real mobile money transaction logs (aggregated from an actual African mobile money provider) without exposing real customer data. Using it as your seed means your "normal" transactions have realistic statistical properties — transaction amounts follow real-world distributions, transaction types (CASH-IN, CASH-OUT, TRANSFER, PAYMENT, DEBIT) have realistic proportions, and there's already a small amount of genuine fraud labeling built in. This gives your project credibility — anyone reviewing your repo who knows the dataset will recognize it as a legitimate research-grade source, not something you invented.

**Half B — The injected signal (Faker + your own rules)**
Raw PaySim alone isn't enough because you need to control the anomalies to prove your pipeline works. This is where you deliberately inject patterns like:
- Rapid-fire transactions (many transfers from one account in a short window)
- Sudden large-amount transactions relative to account history
- Odd-hour activity (e.g., 3am transfers)
- Geographically impossible sequences (if you add a location field)

You inject these knowingly, which means later, when Spark flags them, you can prove — with a clean before/after — that your detection logic works. This "ground truth" is what separates a portfolio project from a toy script.

### Why Kafka specifically (and not just writing to Postgres directly)
This is a common interview question, so it's worth understanding deeply: Kafka acts as a durable, replayable buffer between producers and consumers. It decouples "data being generated" from "data being processed" — meaning:
- Multiple consumers (Spark, a future service, a monitoring tool) can all read the same stream independently
- If your Spark job crashes or you want to re-run detection logic with new rules, you can replay the exact same events from Kafka instead of regenerating data
- It mimics exactly how real fintech/telco systems (like Dialog Axiata's own infrastructure) handle transaction/event data at scale

## Section 2: Spark Structured Streaming fraud detection

### Purpose
This is the "brain" of your streaming pipeline — the component that consumes the raw event stream from Kafka and actually makes a decision about each transaction in near real-time. It's the single most technically impressive part of the project because it demonstrates you can process unbounded, continuously arriving data rather than just batch files.

### Why it matters for your target roles
Sysco Labs, Dialog Axiata, and similar companies deal with high-volume, real-time event data (call detail records, payment events, network telemetry). Spark Structured Streaming experience directly maps to what they'd actually want an associate engineer to be productive in within a few months.

### What it technically does
- Subscribes to the transactions.raw Kafka topic as a streaming DataFrame
- Applies windowed aggregations — e.g., "count of transactions per account in the last 5 minutes" — because fraud detection almost always depends on behavior over time, not a single isolated event
- Applies your rule set (the same anomaly patterns you designed and injected in Section 1): rapid-fire counts, amount thresholds relative to account history, odd-hour flags
- Writes two outputs: flagged transactions to Postgres (for the dashboard) and everything to the raw lake (for later batch reconciliation)

### Key concept to understand deeply
Watermarking and late data — in real streaming systems, events can arrive out of order or late. Understanding how Spark handles this (via watermarks) is a common interview topic and shows maturity beyond "I copied a tutorial."

### What "done" looks like
You can inject a known anomaly via your producer and watch it appear in the Postgres flagged table within seconds, with a rule name attached explaining why it was flagged.

## Section 3: Serving layer + raw lake

### Purpose
This section formalizes the medallion/lambda-style split in your architecture: a fast, query-optimized store for "what's happening right now" (Postgres), and a durable, cheap, replayable store for "everything that ever happened" (MinIO/S3 raw lake).

### Why it matters
This is where you demonstrate you understand a core data engineering trade-off: operational databases are optimized for fast reads/writes on recent data, not for storing years of raw history cheaply. Separating these concerns is exactly what real data platforms do (this is essentially the "bronze" layer of a bronze/silver/gold lakehouse pattern).

### What it technically does
- Postgres table flagged_transactions — indexed, small, serves the dashboard directly
- MinIO/S3 bucket storing raw JSON/Parquet of every transaction (flagged or not) partitioned by date — this becomes the single source of truth that Section 4's batch job reads from

### Key concept to understand deeply
Why store raw, unprocessed data at all if you already flagged the important ones? Because business rules change. If next month you invent a smarter fraud rule, you want to reprocess history — impossible if you only kept the flagged subset.

### What "done" looks like
You can query Postgres for currently flagged transactions AND separately browse the raw lake bucket and see every event ever produced, partitioned sensibly by date.

## Section 4: Batch ingestion + Airflow DAG

### Purpose
This section builds the batch side of your project — pulling in reference/historical datasets (Kaggle loan data, CBSL exchange rates) on a schedule, rather than as a live stream. It proves you can handle both paradigms (streaming and batch), which almost every real DE job description asks for explicitly.

### Why it matters
Most companies don't run pure streaming systems — they run hybrid architectures where some data arrives in real time and some arrives as daily/hourly batch loads from external systems (vendor files, government data, partner APIs). Airflow is the de facto standard for orchestrating this.

### What it technically does
- DAG with tasks: download/ingest Kaggle datasets → land in raw lake → trigger dbt run → (later) refresh dashboard
- Scheduled reconciliation task that also periodically summarizes the streaming data (e.g., daily fraud counts) so batch and streaming outputs stay consistent

### Key concept to understand deeply
Idempotency — if a DAG run fails halfway and you re-run it, it should not duplicate data. This is one of the most common Airflow interview questions.

### What "done" looks like
Your DAG runs on a schedule (or manual trigger) in the Airflow UI, shows green across all tasks, and you can explain what happens if a task fails and is retried.

## Section 5: dbt transformations (star schema)

### Purpose
This is where raw, messy data becomes a clean, query-ready dimensional model. It's the section most closely tied to classic "data warehousing" interview questions (star schema, fact vs dimension tables, SCD).

### Why it matters
Dimensional modeling is a fundamental DE skill that transcends any specific tool — knowing why you'd build fact_transactions with foreign keys to dim_customer, dim_date, dim_branch shows you understand how BI tools and analysts actually consume data, not just how to move it around.

### What it technically does
- Staging models: 1:1 clean-up of raw sources (renaming, type casting, deduplication) — no business logic yet
- Marts models: the actual star schema — fact_transactions (or fact_loans) with measures (amount, risk score) and foreign keys to dimension tables
- Tests: not_null, unique, relationships — dbt's built-in data quality framework, applied via a schema.yml file

### Key concept to understand deeply
Why separate staging from marts? Staging isolates you from source system changes (rename a column upstream, fix it in one place); marts are what analysts/BI tools actually query. This separation is a strong signal of dbt maturity.

### What "done" looks like
Running dbt run builds your full star schema with zero errors, and dbt test passes all data quality checks — and you can explain what each test is actually protecting against.

## Section 6: BI dashboard

### Purpose
This is where all your engineering work becomes visible and tangible — the part a non-technical interviewer or hiring manager will actually look at first. A good dashboard turns your project from "a pile of scripts" into "a product."

### Why it matters
Even though dashboarding isn't strictly a "data engineering" skill, being able to say "I built the pipeline and proved it works by shipping a dashboard on top of it" makes your project demo-able in 5 minutes, which matters enormously in interviews.

### What it technically does
- Connects Superset/Metabase to your Postgres serving layer (real-time fraud view) and your warehouse (historical risk/loan trends)
- Key visuals: fraud flags over time, flagged-by-rule breakdown, loan default rates by segment, exchange rate trend as a reference dimension

### What "done" looks like
A live dashboard you can screen-share, showing both the "live" fraud feed updating and the historical warehouse-driven analytics.

## Section 7: Testing, README, and demo polish

### Purpose
This section is where you turn the project from "code that works on my machine" into something a hiring manager can evaluate in under 10 minutes without needing to run anything.

### Why it matters
Recruiters and technical interviewers at companies like Sysco Labs/Dialog Axiata often skim GitHub repos before an interview. A messy repo with no README undersells even excellent engineering work; a clear, well-documented one overperforms mediocre code.

### What it involves
- Unit tests for your fraud rules and generator logic (even 5-10 solid tests show engineering discipline)
- A README with: architecture diagram (reuse what we built earlier), setup instructions, a short GIF/screenshot of the dashboard, and a "design decisions" section explaining why you made key choices (Kafka vs direct writes, star schema vs flat table, etc.)
- Optionally, a short architecture decision record (ADR) — a one-page doc per major decision, which is a real practice at mature engineering orgs and a nice thing to mention in an interview
