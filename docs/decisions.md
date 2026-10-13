1. Why does PySpark need Java? ✅ Right idea. Fuller answer:

“Spark’s engine is written in Scala and runs on the JVM. PySpark is only a Python API: my Python code sends instructions to a Java process through a bridge called Py4J, and the JVM does the real work. So Java must be installed wherever PySpark runs locally. On Databricks it’s preinstalled.”

2. Why Postgres in Docker, but Spark native? We covered this earlier, so here’s the short version to remember:

“Postgres is a service: I start it once and leave it running, and Docker keeps it isolated and easy to reset. Spark code is what I edit and rerun constantly, so running it natively gives faster runs and easier debugging in VS Code. And since production is Databricks, not a container, a Spark container wouldn’t make local development any more realistic.”

Rule of thumb: services in Docker, code you’re actively writing runs natively.

3. .gitignore and .env. ✅ Correct. Add the why:

“Anything pushed to GitHub stays in the Git history forever, even if you delete it later, so a leaked password must be treated as compromised. .env holds secrets and machine-specific settings, so it’s ignored, and .env.example is committed instead to show which settings are needed, without real values.”

4. Why 5 files, and what is _delta_log?

“Spark splits data into partitions so it can work in parallel, and each partition writes its own file. With local[*], Spark created a partition per CPU core, so even 5 rows became several small files. Too many small files slows reads, which is why Delta has OPTIMIZE to compact them.”

“_delta_log is Delta’s transaction log. Every write adds a numbered JSON file listing which Parquet files were added or removed. Readers use the log to see a consistent version of the table, which gives Delta ACID transactions, MERGE, and time travel. Plain Parquet has no log, so it can’t do any of that.”

# Experiment

- I defined the schema once in SQLAlchemy, so the same code creates it in PostgreSQL locally and Azure SQL in the cloud. SQLAlchemy translates types like Boolean into BOOLEAN or BIT for each database.

# why is program_course unique on program_id + course_id, and not on course_id alone?

- A course can belong to many programs, so course_id alone isn’t unique. But the same course shouldn’t be added twice to the same program, so the combination program_id + course_id is unique. That’s a composite unique constraint on a many-to-many bridge table: program_course connects programs and courses.
