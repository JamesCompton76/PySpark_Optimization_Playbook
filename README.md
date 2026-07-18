# PySpark_Join_Optimizations
A collection of utility scripts and architectural patterns for mitigating data skew and managing cluster memory in distributed Apache Spark environments.

## The Optimization Hierarchy: Broadcast vs. Salting
Before applying complex transformations, modern data pipelines should adhere to a strict join strategy to maximize cluster efficiency:

1. **Broadcast First:** If the dimension table is small enough to fit into memory, always force a Broadcast Join. This copies the lookup table directly to all worker nodes, bypassing the Hash Partitioner entirely. Negating the network shuffle means data skew becomes completely architecturally irrelevant—no salting required.
2. **Salt When Necessary:** If the dimension table is a "Monster Dimension" (e.g., millions of IoT sensors or global customers) that exceeds driver memory limits, broadcasting will cause an Out of Memory (OOM) error. The engine must fall back to a Sort-Merge Join. If the fact data is heavily skewed, Spark's strict "one key to one node" hashing rule will bottleneck and crash the cluster. Only in this scenario do we implement Index Salting to forcibly distribute the processing of that specific key across more allocated memory and CPU cores.

## PySpark Scripts

### 1. Broadcast Strategies

1. **[broadcast_join.py](./broadcast_join.py)** - One-click implementation to force a broadcast join on small lookup tables.
   
   <details>
   <summary><i>Click to expand architectural breakdown</i></summary>

   A lightweight implementation that explicitly hints the Catalyst Optimizer to push a dimension table to all worker nodes.
   
   * **Execution Profiling:** By eliminating the shuffle phase entirely, the pipeline avoids the most expensive network operations in distributed computing.
   * **Execution Note:** Only use this when the dimension table is comfortably smaller than the `spark.sql.autoBroadcastJoinThreshold` (default 10MB, safely configurable up to ~1GB depending on cluster memory limits).
   </details>

### 2. Salting Strategies

1. **[manual_salting_single.py](./manual_salting_single.py)** - Targeted index salting for a single heavily skewed key.
   
   <details>
   <summary><i>Click to expand architectural breakdown</i></summary>

   An optimization script that applies a random salt exclusively to a known skewed key, preventing memory bloat in the dimension table.

   * **Targeted Execution:** Uses `when().otherwise()` logic to isolate the random integer generation to the problematic key.
   * **Memory Management:** Explodes only the specific corresponding row in the dimension table, ensuring normal keys are not artificially multiplied.
   * **Use Case:** Ideal for pipelines where a single entity (like a massive facility or headquarters) generates the vast majority of telemetry.
   </details>

2. **[manual_salting_multiple.py](./manual_salting_multiple.py)** - Targeted index salting for multiple skewed keys using a uniform salt factor.
   
   <details>
   <summary><i>Click to expand architectural breakdown</i></summary>

   Expands the targeted salting approach to handle multiple heavy-hitters cleanly using `.isin()` filtering.

   * **Dynamic Filtering:** Cleanly maps the targeted list across both the fact and dimension tables, avoiding massive chained `when` statements.
   * **Guaranteed Resource Cleanup:** Ensures the inverse records are safely isolated using the `~` operator and unioned back into the dataframe without missing data.
   </details>

3. **[manual_salting_dynamic.py](./manual_salting_dynamic.py)** - Advanced dynamic salting assigning specific bin counts based on data volume.
   
   <details>
   <summary><i>Click to expand architectural breakdown</i></summary>

   The highest level of cluster memory optimization, assigning precise CPU and RAM resources to skewed keys proportional to their actual data volume.

   * **Variable Replication:** Replaces static list comprehensions with dynamic SQL `sequence()` functions to generate variable-length arrays row-by-row in the dimension table.
   * **Use Case:** Essential for multi-terabyte pipelines operating under strict, cost-constrained cluster environments where blanket salting would cause Out of Memory (OOM) failures on smaller heavy-hitters.
   * **Execution Note:** Requires prior knowledge of data distribution or a pre-query to calculate the required bin map dynamically.
   </details>

## Architectural Notes & Memory Management

* **How Broadcast Joins Work:** When a broadcast join is triggered, Spark pulls the entire dimension table into the **Driver Node's** memory first. The Driver then serializes it and broadcasts a complete, identical copy to the memory of every individual **Worker Node** in the cluster. This allows the worker nodes to join the fact table locally without executing an expensive network shuffle.
* **What is "Small Enough"?** By default, Spark's `spark.sql.autoBroadcastJoinThreshold` is set to exactly 10MB. However, you can safely tune this threshold up to 1GB or even 2GB, provided your table is genuinely that small when compressed in memory *and* your Driver Node has enough RAM allocated to handle it.
* **When to Allocate More Memory:** If you have a "Monster Dimension" (e.g., a 4GB customer table) that you desperately want to broadcast to avoid a catastrophic Sort-Merge Join, you must explicitly increase the Driver's memory allocation (`spark.driver.memory`). If you cannot physically allocate enough Driver RAM to hold the table, broadcasting will crash the pipeline with an Out of Memory (OOM) error, and you must fall back to Targeted Salting.
* **Enterprise Execution (Palantir Foundry):** In managed enterprise platforms like Palantir Foundry, hardcoding cluster configurations via `spark.driver.memory` or `spark.sql.shuffle.partitions` inside the Python logic is considered an anti-pattern. Instead, infrastructure scaling is decoupled from the code. Driver and executor memory are scaled by assigning specific **Spark Profiles** (e.g., `EXECUTOR_MEMORY_LARGE` or `SHUFFLE_PARTITIONS_HIGH`) directly to the dataset via the UI, keeping the actual transformation scripts purely focused on data logic.

## Diagnosis, Edge Cases, & Adaptive Query Execution (AQE)

* **How to Diagnose Data Skew (The Spark UI):** Before implementing salting, a data engineer must confirm that data skew is the root cause. The telltale signs appear directly in the **Spark UI**. When viewing the Execution Stage tab, look for the "Straggler Task"—a stage with hundreds of tasks where 99% complete in seconds, but one or two tasks remain active for tens of minutes. If the *Median* task duration is 15 seconds, but the *Max* task duration is 45 minutes, a worker node is bottlenecked by a massive, single partition skew.
* **The Native Alternative (AQE):** Introduced dynamically in Spark 3.x, **Adaptive Query Execution (AQE)** features built-in skew mitigation that can be enabled via cluster configurations (`spark.sql.adaptive.skewJoin.enabled`). During runtime, AQE monitors shuffle statistics and automatically splits exceptionally large partitions into smaller sub-partitions. While this is a highly effective safety net, manual targeted salting is still essential. AQE fails on unsupported join types (e.g., Full Outer Joins), struggles with complex, multi-key skews, and relies heavily on tuning secondary parameters. Manual salting guarantees deterministic, optimized data distribution regardless of cluster engine overrides.
* **The Silent Killer ("Null" Key Skew):** One of the most frequent traps in big data engineering is skew caused by missing data rather than massive assets. When millions of rows have a missing or `NULL` join key, Spark’s Hash Partitioner streams every single one of those rows to the exact same partition. Always explicitly strip out or isolate `NULL`/blank join keys from the fact table *prior* to executing an inner join condition, completely bypassing both the shuffle and the need to apply computational overhead via salting.
