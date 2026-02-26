 1. Dry run mode (--dry-run --limit N) — do this first

  Lowest effort of the P0s. The plumbing already exists — source reading, transform pipeline, Arrow batch flow. You just need to:
  - Skip destination instantiation
  - Cap source reads at --limit N
  - Pretty-print the resulting batches to stdout

  This immediately improves the dev loop for anyone building pipelines. A few days of work, max.

  2. pgoutput CDC plugin — biggest production unlock

  test_decoding works but is a dealbreaker for managed Postgres (RDS, Cloud SQL, Aurora, Supabase) where you can't install extensions. pgoutput is built-in everywhere. This is the
  single biggest gate to real-world CDC adoption.

  It's also the hardest P0 — binary protocol parsing, relation message handling, streaming transactions, TOAST edge cases. Worth starting soon since it has the longest tail.

  3. DataFusion SQL transforms — the differentiator

  This is what makes Rapidbyte more than "just another ELT tool." In-flight SQL transforms on Arrow batches before data hits the warehouse eliminates the Fivetran+dbt two-step. The
  Arrow foundation is already there, DataFusion plugs in naturally.

  4. S3 state backend — defer slightly

  SQLite and Postgres state backends cover most deployment scenarios already. S3 matters for ephemeral/serverless, but teams running Rapidbyte today likely have a Postgres instance
  available. Do this after the above three.
