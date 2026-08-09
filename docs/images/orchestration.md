# Orchestration

`docs/orchestration.md`

The pipeline runs as one Databricks Workflow, `Kestrel_ETL_Pipeline`, on serverless compute. It finds its own work, processes a batch end to end, and records what it did.

![Job workflow](images/workflow.svg)

---

## Requirements

- Find the next unprocessed batch without being told which one
- Process batches oldest first, so ingestion follows the order the source delivered
- Finish successfully when there is nothing to do, rather than failing
- Resolve the batch identifier once and pass it to every task
- Keep batch state visible and queryable at all times
- Return a failed batch to the queue instead of letting it block the pipeline
- Never process the same batch twice

---

## Control table

`control.batch_control` is the pipeline's memory. Nothing is held between runs in code, because the table records what has already been handled.

| Column | Purpose |
|---|---|
| `batch_id` | The batch folder being processed |
| `status` | `in-progress`, `completed` or `failed` |
| `created_timestamp` | When the batch was claimed |
| `updated_timestamp` | When its status last changed |
| `error_message` | Populated when a run fails |

Rows are appended rather than overwritten, so a batch that took several attempts keeps its full history.

---

## Tasks

Nine tasks. Every task from `create_new_batch` down takes `p_batch_id` as a parameter, sourced from the first task.

| Task | Type | Depends on | Parameter |
|---|---|---|---|
| `identify_next_batch` | Notebook | — | none |
| `check_has_batch` | If/else condition | `identify_next_batch` | — |
| `create_new_batch` | Notebook | `check_has_batch` (true) | `p_batch_id` |
| `run_bronze_dimensions` | Notebook | `create_new_batch` | `p_batch_id` |
| `run_bronze_incremental_facts` | Notebook | `run_bronze_dimensions` | `p_batch_id` |
| `run_silver` | Notebook | `run_bronze_incremental_facts` | `p_batch_id` |
| `run_gold_dimensions` | Notebook | `run_silver` | `p_batch_id` |
| `run_gold_facts` | Notebook | `run_gold_dimensions` | `p_batch_id` |
| `complete_batch` | Notebook | `run_gold_facts` | `p_batch_id` |
| `fail_batch` | Notebook | all four processing tasks | `p_batch_id` |

**Condition expression**

```
{{tasks.identify_next_batch.values.has_batch}} == true
```

**Parameter passed to every downstream task**

```
p_batch_id = {{tasks.identify_next_batch.values.p_batch_id}}
```

`fail_batch` uses **Run if dependencies: at least one failed**, so it fires only when something upstream breaks.

---

## What each run does

`identify_next_batch` lists the landing volume, keeps only directories, and compares them against batches the control table already holds at `in-progress` or `completed`. The earliest untracked batch wins. Two task values are published: `p_batch_id` with the batch, and `has_batch` as a flag.

`check_has_batch` routes on that flag. With nothing new, the run ends green rather than failing, which matters for a scheduled job that fires whether or not files have arrived.

`create_new_batch` writes the batch to the control table at `in-progress`, claiming it.

The four processing tasks run in sequence: bronze dimensions, bronze facts, silver, gold dimensions, gold facts. Order matters in gold, since facts take their surrogate keys from the dimensions.

`complete_batch` merges the batch to `completed` using a condition requiring the current status to be `in-progress`. A batch can therefore only be completed if it was properly started, and re-running the task cannot alter a finished row.

`fail_batch` marks the batch `failed` with an error message. Because `identify_next_batch` treats only `in-progress` and `completed` as tracked, a failed batch becomes eligible again on the next run with no manual intervention.

---

## Two details worth knowing

**`has_batch` is a string, not a boolean.** The condition task compares values as text, so a Python boolean would serialise as `False` with a capital F and the comparison would never match.

**Layer notebooks are chained with `%run`, not `dbutils.notebook.run`.** Databricks Free Edition allows one concurrent run, and `dbutils.notebook.run` creates a nested run, which fails immediately with `REQUEST_LIMIT_EXCEEDED`. `%run` executes inline in the same context, so no new run is created. On a paid workspace each notebook would be its own job task.

---

## Limitations

**One batch per run.** A backlog clears over successive runs. Iterating the full list of new batches through a For Each task would clear it in one.

**A cancelled run leaves the batch stranded.** `fail_batch` covers a task failing, but if the run is cancelled or the cluster dies, no task executes and the batch stays at `in-progress`, which the discovery step reads as tracked. A stale-reset at the top of `identify_next_batch`, flipping any `in-progress` row older than a few hours to `failed`, would close this.

**Concurrency must stay capped at one run.** Two overlapping runs could both read the landing volume before either writes its `in-progress` row and claim the same batch.
