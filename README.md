# PySpark_Join_Optimizations
A collection of utility scripts and architectural patterns for mitigating data skew and managing cluster memory in distributed Apache Spark environments.

## The Optimization Hierarchy: Broadcast vs. Salting
Before applying complex transformations, modern data pipelines should adhere to a strict join strategy to maximize cluster efficiency:

1. **Broadcast First:** If the dimension table is small enough to fit into memory, always force a Broadcast Join. This copies the lookup table directly to all worker nodes, bypassing the Hash Partitioner entirely. Negating the network shuffle means data skew becomes completely architecturally irrelevant—no salting required.
2. **Salt When Necessary:** If the dimension table is a "Monster Dimension" (e.g., millions of IoT sensors or global customers) that exceeds driver memory limits, broadcasting will cause an Out of Memory (OOM) error. The engine must fall back to a Sort-Merge Join. If the fact data is heavily skewed, Spark's strict "one key to one node" hashing rule will bottleneck and crash the cluster. Only in this scenario do we implement Index Salting to forcibly distribute the processing of that specific key across more allocated memory and CPU cores.

## PySpark Scripts

1. **[broadcast_join.py](./broadcast_join.py)** - One-click implementation to force a broadcast join on small lookup tables.
   
   <details>
   <summary><i>Click to expand architectural breakdown</i></summary>

   A lightweight implementation that explicitly hints the Catalyst Optimizer to push a dimension table to all worker nodes.
   
   * **Execution Profiling:** By eliminating the shuffle phase entirely, the pipeline avoids the most expensive network operations in distributed computing.
   * **Execution Note:** Only use this when the dimension table is comfortably smaller than the `spark.sql.autoBroadcastJoinThreshold` (default 10MB, safely configurable up to ~1GB depending on cluster memory limits).
   </details>

2. **[manual_salting_single.py](./manual_salting_single.py)** - Targeted index salting for a single heavily skewed key.
   
   <details>
   <summary><i>Click to expand architectural breakdown</i></summary>

   An optimization script that applies a random salt exclusively to a known skewed key, preventing memory bloat in the dimension table.

   * **Targeted Execution:** Uses `when().otherwise()` logic to isolate the random integer generation to the problematic key.
   * **Memory Management:** Explodes only the specific corresponding row in the dimension table, ensuring normal keys are not artificially multiplied.
   * **Use Case:** Ideal for pipelines where a single entity (like a massive facility or headquarters) generates the vast majority of telemetry.
   </details>

3. **[manual_salting_multiple.py](./manual_salting_multiple.py)** - Targeted index salting for multiple skewed keys using a uniform salt factor.
   
   <details>
   <summary><i>Click to expand architectural breakdown</i></summary>

   Expands the targeted salting approach to handle multiple heavy-hitters cleanly using `.isin()` filtering.

   * **Dynamic Filtering:** Cleanly maps the targeted list across both the fact and dimension tables, avoiding massive chained `when` statements.
   * **Guaranteed Resource Cleanup:** Ensures the inverse records are safely isolated using the `~` operator and unioned back into the dataframe without missing data.
   </details>

4. **[manual_salting_dynamic.py](./manual_salting_dynamic.py)** - Advanced dynamic salting assigning specific bin counts based on data volume.
   
   <details>
   <summary><i>Click to expand architectural breakdown</i></summary>

   The highest level of cluster memory optimization, assigning precise CPU and RAM resources to skewed keys proportional to their actual data volume.

   * **Variable Replication:** Replaces static list comprehensions with dynamic SQL `sequence()` functions to generate variable-length arrays row-by-row in the dimension table.
   * **Use Case:** Essential for multi-terabyte pipelines operating under strict, cost-constrained cluster environments where blanket salting would cause Out of Memory (OOM) failures on smaller heavy-hitters.
   * **Execution Note:** Requires prior knowledge of data distribution or a pre-query to calculate the required bin map dynamically.
   </details>[cite: 11]
