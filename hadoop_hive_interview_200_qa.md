# 200 Hadoop & Hive Interview Questions for Data Engineers (2 Years Experience)

Real-industry, scenario-driven questions with answers an experienced professional would give.

## Table of Contents

| Section | Questions |
|---|---|
| 1. Hadoop Fundamentals | Q1 – Q10 |
| 2. HDFS | Q11 – Q50 |
| 3. YARN | Q51 – Q68 |
| 4. MapReduce | Q69 – Q85 |
| 5. Hive Architecture & Basics | Q86 – Q105 |
| 6. Hive Tables, Partitioning, Bucketing, File Formats | Q106 – Q131 |
| 7. HiveQL: Joins, Functions, Window Functions, Query Scenarios | Q132 – Q160 |
| 8. Hive Performance Tuning | Q161 – Q175 |
| 9. Hive ACID, UDFs, Security, Ecosystem (Sqoop, Oozie, HBase, Spark, Cloud) | Q176 – Q188 |
| 10. Real Industry Scenarios | Q189 – Q200 |

---

# SECTION 1: HADOOP FUNDAMENTALS

### Q1. What is Hadoop and what problems does it solve?
**Answer:** Hadoop is an open-source framework for storing and processing very large datasets across clusters of commodity machines. It solves three problems: storage that exceeds a single machine (HDFS), processing that exceeds a single machine (MapReduce/YARN), and fault tolerance on cheap hardware (replication and task re-execution). The core idea is moving computation to the data rather than moving data to the computation.

---

### Q2. What are the core components of Hadoop 2.x/3.x?
**Answer:**
- **HDFS**: distributed storage.
- **YARN**: resource management and job scheduling.
- **MapReduce**: batch processing engine.
- **Hadoop Common**: shared libraries and utilities.

Ecosystem tools (Hive, Spark, HBase, Sqoop, Oozie, Kafka) run on top of or alongside these.

---

### Q3. What are the 3 Vs (or 5 Vs) of big data?
**Answer:** Volume (size), Velocity (speed of arrival), Variety (structured, semi-structured, unstructured). Extended: Veracity (quality and trust) and Value. Interviewers use this to check that you can connect the concepts to design decisions, such as batch vs streaming for velocity and schema-on-read for variety.

---

### Q4. What is the difference between Hadoop 1.x, 2.x, and 3.x?
**Answer:**
- **1.x**: JobTracker/TaskTracker, MapReduce only, single NameNode (single point of failure), limited scalability (~4,000 nodes).
- **2.x**: YARN separates resource management from processing, NameNode HA, federation, multiple engines (Spark, Tez).
- **3.x**: erasure coding, multiple standby NameNodes, Java 8 minimum, YARN timeline service v2, GPU support, and default ports changed.

---

### Q5. What does "move computation to data" mean?
**Answer:** Instead of pulling terabytes across the network to a compute node, Hadoop schedules tasks on (or near) the node holding the block. This is called **data locality**: node-local is best, then rack-local, then off-rack. It reduces network I/O, which is the main bottleneck in large clusters.

---

### Q6. What is commodity hardware, and how does Hadoop tolerate its failures?
**Answer:** Inexpensive, standard servers rather than specialized machines. Failures are expected, so HDFS replicates blocks (default factor 3), the NameNode re-replicates under-replicated blocks, and YARN re-runs failed tasks on other nodes. Failure handling is built into the software, not the hardware.

---

### Q7. Schema-on-read vs schema-on-write?
**Answer:** Schema-on-write (RDBMS): data is validated and shaped when loaded, so loads are slower but queries are fast and consistent. Schema-on-read (Hive/Hadoop): raw files are stored as-is and a schema is applied at query time, so loads are fast and flexible, but bad data surfaces later as NULLs or parse errors. Good practice: keep raw data immutable, then publish curated, typed tables.

---

### Q8. When would you NOT use Hadoop?
**Answer:** Low-latency transactional workloads (OLTP), small datasets that fit on one machine, many small files, workloads needing frequent random updates, and real-time ad hoc queries where a warehouse or an MPP engine is a better fit. Today many teams choose cloud object storage with Spark, Trino, or Snowflake instead of building new on-prem Hadoop.

---

### Q9. Hadoop vs traditional RDBMS?
**Answer:**
| Aspect | Hadoop | RDBMS |
|---|---|---|
| Scale | Horizontal, petabytes | Vertical, mostly terabytes |
| Schema | On read | On write |
| Data | Structured and unstructured | Structured |
| Access | Batch, high throughput | Interactive, low latency |
| Updates | Append-oriented | Row-level updates |
| Cost | Commodity hardware | Expensive hardware/licences |

---

### Q10. How do Hadoop distributions and modern platforms differ (Cloudera, EMR, Dataproc, HDInsight, Databricks)?
**Answer:** On-prem distributions (Cloudera CDP) bundle and support HDFS, YARN, Hive, and so on. Cloud services (EMR, Dataproc, HDInsight) run the same open-source stack on managed clusters, usually with object storage (S3, GCS, ADLS) replacing HDFS and ephemeral clusters replacing permanent ones. Databricks and similar platforms replace Hive/YARN with managed Spark and a metastore, while keeping Hive-compatible catalogs and file formats.

---

# SECTION 2: HDFS

### Q11. Explain HDFS architecture.
**Answer:** Master/worker design. The **NameNode** keeps the filesystem namespace and block-to-DataNode mapping in memory. **DataNodes** store the actual blocks, send heartbeats (every 3 seconds) and block reports to the NameNode. Clients ask the NameNode for block locations and then read/write directly with DataNodes. Data never flows through the NameNode.

---

### Q12. What is the default block size and why is it so large?
**Answer:** 128 MB in Hadoop 2.x/3.x (64 MB in 1.x). Large blocks reduce the amount of metadata the NameNode holds, reduce seek overhead relative to transfer time, and match HDFS's sequential-read design. Block size is configurable per file (`dfs.blocksize`).

---

### Q13. What is the replication factor and how do you change it?
**Answer:** The number of copies of each block (default 3). Change the default with `dfs.replication` or per file/directory:
```bash
hdfs dfs -setrep -w 2 /data/archive/2024
```
Lower replication saves space for cold data; higher replication helps heavily read data such as small lookup tables.

---

### Q14. Explain HDFS block placement (rack awareness).
**Answer:** The default policy: replica 1 on the writer's node (or a random node if the client is outside the cluster), replica 2 on a node in a different rack, and replica 3 on a different node in the same rack as replica 2. This survives the loss of a node or a whole rack while limiting cross-rack write traffic. A topology script configured by the admin tells Hadoop which rack each node belongs to.

---

### Q15. What are fsimage and edit logs?
**Answer:** `fsimage` is a point-in-time snapshot of the namespace. The `edits` log records every change since that snapshot. On startup the NameNode loads fsimage and replays edits to rebuild the in-memory state. Periodic checkpointing merges edits into a new fsimage so edits don't grow without bound.

---

### Q16. What does the Secondary NameNode do? Is it a backup?
**Answer:** No, it's not a hot standby. It periodically downloads fsimage and edits from the NameNode, merges them into a new fsimage (a checkpoint), and sends it back. This keeps restart time short. For real failover, use NameNode High Availability with a Standby NameNode.

---

### Q17. How does NameNode High Availability work?
**Answer:** An Active and a Standby NameNode share edit logs through a quorum of **JournalNodes** (odd number, at least 3). DataNodes send block reports to both. **ZooKeeper Failover Controllers (ZKFC)** monitor health and perform automatic failover, and **fencing** ensures the old Active can no longer write (prevents split-brain). Hadoop 3 supports more than one standby.

---

### Q18. What is HDFS Federation?
**Answer:** Multiple independent NameNodes, each managing a namespace volume (a portion of the filesystem), sharing the same pool of DataNodes. It scales the namespace beyond one NameNode's memory and isolates workloads. Clients use ViewFS or mount tables to see a unified path.

---

### Q19. Walk through the HDFS write path.
**Answer:**
1. Client asks the NameNode to create the file; NameNode checks permissions and records it.
2. The client writes data to a **pipeline** of DataNodes chosen by the NameNode (e.g., DN1 → DN2 → DN3), in packets.
3. Each DataNode forwards to the next and ACKs back up the pipeline.
4. When a block fills, the client requests the next block from the NameNode.
5. On close, the NameNode commits the file once the minimum replication is met.

---

### Q20. Walk through the HDFS read path.
**Answer:** The client asks the NameNode for block locations (sorted by proximity). It then reads each block directly from the closest DataNode, verifying checksums. If a DataNode fails or a checksum fails, the client switches to another replica and reports the corrupt block to the NameNode.

---

### Q21. What happens if a DataNode fails during a write?
**Answer:** The pipeline is rebuilt without the failed node; the client continues writing to the remaining DataNodes. The NameNode later notices the block is under-replicated and schedules re-replication to restore the replication factor.

---

### Q22. How does HDFS detect a dead DataNode?
**Answer:** DataNodes send heartbeats every 3 seconds. If the NameNode doesn't hear from a DataNode for roughly 10.5 minutes (2 × recheck interval + 10 × heartbeat), it marks the node dead and re-replicates its blocks.

---

### Q23. How does HDFS ensure data integrity?
**Answer:** Checksums (CRC32C) are computed per chunk (default 512 bytes) on write and verified on read. DataNodes also run a periodic **block scanner**. Corrupt replicas are reported and replaced from healthy copies.

---

### Q24. What is Safe Mode?
**Answer:** A read-only startup state where the NameNode waits for enough DataNodes to report blocks (default: 99.9% of blocks meeting minimal replication) before allowing writes.
```bash
hdfs dfsadmin -safemode get
hdfs dfsadmin -safemode leave
```
Don't force-leave without investigating missing blocks.

---

### Q25. Scenario: after a maintenance reboot, the NameNode is stuck in safe mode. What do you check?
**Answer:** Check the NameNode UI/logs for "reported blocks X needs additional Y blocks". Verify all DataNodes are up and reporting (`hdfs dfsadmin -report`), check disks and network, and run `hdfs fsck / -list-corruptfileblocks` for missing blocks. If blocks are permanently lost, decide whether to restore from backup or delete the corrupt files before leaving safe mode.

---

### Q26. Common HDFS shell commands you use daily?
**Answer:**
```bash
hdfs dfs -ls -h /data                  # list
hdfs dfs -put local.csv /data/raw/     # upload
hdfs dfs -get /data/out/part-0000 .    # download
hdfs dfs -cat /data/f | head
hdfs dfs -du -s -h /data/*             # space used
hdfs dfs -mkdir -p /data/raw/dt=2026-10-25
hdfs dfs -mv, -cp, -rm -r -skipTrash
hdfs dfs -chmod 750 /data; hdfs dfs -chown etl:analytics /data
hdfs dfs -getmerge /data/out merged.txt
```

---

### Q27. `put` vs `copyFromLocal` vs `moveFromLocal`; `get` vs `copyToLocal`?
**Answer:** `put` and `copyFromLocal` copy from local to HDFS (the latter restricts the source to local files). `moveFromLocal` deletes the local source after copying. `get` and `copyToLocal` copy from HDFS to local.

---

### Q28. What is the HDFS Trash?
**Answer:** A recycle-bin feature (`fs.trash.interval` in minutes; 0 means disabled). Deleted files move to `/user/<name>/.Trash` and are purged after the interval. Use `-skipTrash` to bypass it. Always enable trash in production, because an accidental `rm -r` is otherwise unrecoverable.

---

### Q29. What is `distcp` and when do you use it?
**Answer:** A MapReduce-based tool for large intra- or inter-cluster copies, parallelized across many mappers.
```bash
hadoop distcp -update -delete -m 50 hdfs://nn1/data hdfs://nn2/data
hadoop distcp hdfs:///data/dt=2026-10-24 s3a://bucket/archive/
```
Used for cluster migrations, DR copies, and moving data to cloud storage.

---

### Q30. What are HDFS snapshots?
**Answer:** Read-only point-in-time copies of a directory, created cheaply (copy-on-write metadata, no data duplication).
```bash
hdfs dfsadmin -allowSnapshot /data/warehouse
hdfs dfs -createSnapshot /data/warehouse before_migration
```
Good for protection against accidental deletes and for consistent backups.

---

### Q31. What is the small files problem in HDFS?
**Answer:** Each file, directory, and block costs roughly 150 bytes of NameNode heap. Millions of tiny files exhaust NameNode memory and create too many map tasks, since each small file gets its own split. Solutions: merge files (compaction jobs, Hive `CONCATENATE`, Spark `coalesce`), use container formats (SequenceFile, Parquet/ORC), HAR files, and `CombineFileInputFormat`, and fix the ingestion pattern that produces them.

---

### Q32. Scenario: your streaming ingestion writes 10,000 small files per hour to HDFS. How do you fix it?
**Answer:** Increase batch intervals or roll files by size/time (e.g., Flume/Kafka Connect HDFS sink rolling at 128 MB), land raw data then run a scheduled compaction job that rewrites partitions into fewer large Parquet/ORC files, partition at a sensible granularity (daily, not minutely), and monitor file counts and average file size per partition.

---

### Q33. What is a HAR file?
**Answer:** A Hadoop Archive packs many small files into a single archive (`.har`) to relieve NameNode pressure, while remaining readable via the `har://` scheme. It's read-only and not splittable for efficient MapReduce, so it's suited to cold data rather than active processing.

---

### Q34. Compare HDFS with a local filesystem and with object stores like S3.
**Answer:** HDFS provides POSIX-like semantics (atomic rename, append), data locality, and strong consistency, but needs cluster operations. S3 is cheap, durable, elastic and decoupled from compute, but rename is copy-and-delete (slow, non-atomic), listing can be slow, and there's no data locality. Committers (S3A magic committer), table formats (Iceberg, Delta), and tuned `fs.s3a` settings address these issues.

---

### Q35. What are the HDFS file permissions model and ACLs?
**Answer:** POSIX-style owner/group/other with `rwx`, plus extended ACLs for finer control (`hdfs dfs -setfacl -m user:alice:r-x /data/finance`). In secured clusters, Kerberos authenticates users, and Apache Ranger (or Sentry in older stacks) provides central authorization and audit.

---

### Q36. How is HDFS secured?
**Answer:** Kerberos for authentication, HDFS permissions/ACLs/Ranger for authorization, TLS for data in transit (and SASL data transfer protection), HDFS Transparent Encryption (encryption zones with keys in Hadoop KMS) for data at rest, and audit logging.

---

### Q37. What are HDFS encryption zones?
**Answer:** Directories whose files are transparently encrypted/decrypted by the client using per-file data encryption keys, which are wrapped by a zone key held in the KMS. The data stays encrypted on DataNodes, and HDFS admins can't read plaintext without KMS access.

---

### Q38. What is erasure coding in Hadoop 3?
**Answer:** Instead of 3 full copies (200% overhead), data is split into data and parity blocks (e.g., RS-6-3: 6 data + 3 parity = 50% overhead) and survives multiple failures. It saves roughly half the storage for cold data, at the cost of more CPU and network for reconstruction and no data locality for reads. Apply it to infrequently accessed data: `hdfs ec -setPolicy -path /data/cold -policy RS-6-3-1024k`.

---

### Q39. What is the HDFS Balancer, and when do you run it?
**Answer:** It moves blocks between DataNodes to even out disk usage after adding nodes or after skewed writes: `hdfs balancer -threshold 10`. Throttle bandwidth (`dfs.datanode.balance.bandwidthPerSec`) so it doesn't disrupt production jobs. The Disk Balancer handles imbalance between disks inside one DataNode.

---

### Q40. What does `hdfs fsck` do?
**Answer:** Checks filesystem health: missing, corrupt, and under-replicated blocks.
```bash
hdfs fsck /data -files -blocks -locations
hdfs fsck / -list-corruptfileblocks
```
It's a read-only report by default; `-delete` and `-move` change state, so use them with caution.

---

### Q41. Scenario: `fsck` reports under-replicated blocks. What do you do?
**Answer:** Check whether a DataNode or rack recently went down (`hdfs dfsadmin -report`). If the nodes are back, the NameNode re-replicates automatically; watch the "under-replicated blocks" counter fall. If it's stuck, check disk space, `dfs.namenode.replication.work.multiplier.per.iteration`, network, and whether the replication factor exceeds the number of live nodes.

---

### Q42. Scenario: HDFS is 90% full. What are your options?
**Answer:** Find the large directories (`hdfs dfs -du -h /`), delete expired/temp/staging data and `.Trash`, clean old Hive scratch and Spark event logs, lower replication or apply erasure coding for cold data, compress/convert text to ORC/Parquet, archive to cheaper storage (S3/Glacier), then add nodes and run the balancer. Set quotas and retention policies to prevent recurrence.

---

### Q43. What are HDFS quotas?
**Answer:** Limits on a directory: a name quota (number of files/directories) and a space quota (bytes, counting replicas).
```bash
hdfs dfsadmin -setQuota 1000000 /user/etl
hdfs dfsadmin -setSpaceQuota 10t /user/etl
```
They protect the cluster from runaway jobs and from the small files problem.

---

### Q44. Which file formats and compression codecs work best with HDFS?
**Answer:** Columnar formats (ORC, Parquet) for analytics; Avro for row-oriented/streaming with schema evolution; SequenceFile for intermediate MapReduce data. Codecs: Snappy (fast, splittable only inside container formats), LZO (splittable with indexing), gzip (high ratio, not splittable), bzip2 (splittable, slow), zstd (good balance). A non-splittable codec on a giant text file forces one mapper to read the whole file.

---

### Q45. What does "splittable" mean and why does it matter?
**Answer:** A splittable file can be read starting from the middle by multiple parallel tasks. A 10 GB gzip text file is read by a single mapper; a 10 GB ORC or Parquet file (with gzip/snappy inside blocks) is processed by many mappers in parallel.

---

### Q46. How do you check cluster health and capacity?
**Answer:** `hdfs dfsadmin -report` (capacity, DataNodes live/dead), the NameNode web UI (default port 9870 in Hadoop 3, 50070 in 2.x), `yarn node -list`, Cloudera Manager/Ambari dashboards, and metrics in Grafana/Prometheus. Track missing/under-replicated blocks, NameNode heap and GC, RPC latency, and DataNode disk failures.

---

### Q47. What are important NameNode tuning points?
**Answer:** Heap sized for the object count (~1 GB per million blocks as a rough guide), GC settings (G1) to avoid long pauses, `dfs.namenode.handler.count` for RPC concurrency, separate disks and an NFS/JournalNode setup for metadata, and avoiding the small files problem.

---

### Q48. How do you decommission a DataNode safely?
**Answer:** Add the host to the excludes file (`dfs.hosts.exclude`) and run `hdfs dfsadmin -refreshNodes`. The node enters "Decommission in progress" while its blocks are re-replicated elsewhere; remove it only when the status is "Decommissioned". Removing the node abruptly instead risks under-replication and data loss.

---

### Q49. What is HttpFS or WebHDFS?
**Answer:** REST APIs for HDFS operations without needing Hadoop client libraries. WebHDFS talks directly to NameNode/DataNodes; HttpFS is a gateway (single endpoint, useful through firewalls). Example: `curl "http://nn:9870/webhdfs/v1/data?op=LISTSTATUS"`.

---

### Q50. Scenario: a user accidentally deleted a critical directory with `-skipTrash`. How can you recover?
**Answer:** Check HDFS snapshots (`.snapshot` directory) or backups/DR cluster first. If none exist, recovery is very difficult; blocks are removed asynchronously, so stop activity and escalate immediately. Prevention: enable trash, snapshot critical directories, restrict delete permissions (Ranger), and keep DR copies via `distcp`.

---

# SECTION 3: YARN

### Q51. What is YARN and why was it introduced?
**Answer:** Yet Another Resource Negotiator: the cluster resource manager introduced in Hadoop 2. It separated resource management from the MapReduce programming model (which the JobTracker used to do in Hadoop 1), removing a scalability bottleneck and a single point of failure, and allowing many engines (MapReduce, Spark, Tez, Flink) to share one cluster.

---

### Q52. Explain YARN components.
**Answer:**
- **ResourceManager (RM)**: global arbiter; contains the Scheduler (allocates resources) and ApplicationsManager (accepts jobs, starts the ApplicationMaster).
- **NodeManager (NM)**: per-node agent; launches and monitors containers, reports resource usage.
- **ApplicationMaster (AM)**: per-application; negotiates containers and manages task execution and retries.
- **Container**: an allocation of memory and vCores on a node in which a task runs.

---

### Q53. Walk through the lifecycle of a job submitted to YARN.
**Answer:**
1. The client submits the application to the RM.
2. The RM allocates a container and launches the ApplicationMaster on a NodeManager.
3. The AM registers and requests containers (with locality preferences).
4. The RM Scheduler grants containers; the AM contacts the NMs to start tasks.
5. Tasks run and report progress to the AM; the AM handles failures.
6. On completion, the AM unregisters and the containers are released.

---

### Q54. What are the YARN schedulers?
**Answer:**
- **FIFO**: first come, first served; unsuitable for shared clusters.
- **Capacity Scheduler**: hierarchical queues with guaranteed capacities and elasticity; the default in Cloudera/Hortonworks.
- **Fair Scheduler**: shares resources equally across running apps/queues over time, with weights and preemption.

---

### Q55. Capacity vs Fair scheduler: when do you choose which?
**Answer:** Capacity suits multi-tenant organizations that need guaranteed shares per department/team with strict queue ACLs (e.g., ETL gets 60%, BI 30%, data science 10%). Fair suits workloads where you want equitable sharing and small jobs to finish quickly without waiting behind large ones. Both support preemption and queue limits.

---

### Q56. Scenario: ad hoc analyst queries slow down the nightly ETL. How do you protect ETL?
**Answer:** Create separate YARN queues (e.g., `etl`, `adhoc`) with guaranteed capacities and maximum limits, restrict who can submit to each via ACLs, enable preemption so ETL can reclaim its share, set per-user limits in the ad hoc queue (`user-limit-factor`), and route Hive/Spark sessions to queues (`set tez.queue.name=adhoc;`).

---

### Q57. Which key YARN memory settings do you know?
**Answer:**
- `yarn.nodemanager.resource.memory-mb`: total memory per node for containers.
- `yarn.scheduler.minimum-allocation-mb` / `maximum-allocation-mb`: container size bounds.
- `yarn.nodemanager.resource.cpu-vcores`: vCores per node.
- Job-level: `mapreduce.map.memory.mb`, `mapreduce.reduce.memory.mb`, `mapreduce.map.java.opts` (heap about 80% of container size), Spark's `spark.executor.memory` plus overhead.

---

### Q58. A container was killed with "running beyond physical memory limits". Why and how do you fix it?
**Answer:** The task's process memory (heap + off-heap + overhead) exceeded the container size, so the NodeManager killed it. Fix by increasing the container size (`mapreduce.map.memory.mb`) and keeping the heap (`-Xmx`) at about 80% of it; for Spark increase `spark.executor.memoryOverhead`; also reduce per-task data (more partitions/mappers, avoid collecting large data, fix skew).

---

### Q59. What are the YARN application states, and how do you monitor and kill jobs?
**Answer:**
```bash
yarn application -list -appStates RUNNING
yarn application -status application_1700000000000_0042
yarn logs -applicationId application_1700000000000_0042
yarn application -kill application_1700000000000_0042
```
States include NEW, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED. "ACCEPTED" for a long time means no resources in the queue.

---

### Q60. Scenario: your job is stuck in ACCEPTED state. What do you check?
**Answer:** Queue capacity and usage in the RM UI (is the queue full or at its max limit?), the ApplicationMaster resource limit (`maximum-am-resource-percent`), per-user limits, whether NodeManagers are healthy, and resources requested larger than the maximum allocation. Fix by waiting, moving to another queue, shrinking the request, or killing blocking jobs.

---

### Q61. How does ResourceManager HA work?
**Answer:** An Active/Standby RM pair coordinated with ZooKeeper, with application state stored in a state store (ZooKeeper/HDFS). On failover, running ApplicationMasters re-register with the new Active RM, so applications can continue (work-preserving recovery).

---

### Q62. What is YARN log aggregation?
**Answer:** After an application finishes, NodeManagers upload container logs to HDFS (`yarn.log-aggregation-enable=true`), and `yarn logs -applicationId <id>` retrieves them in one place. Without it, logs live only on the local node and vanish when the container directories are cleaned up.

---

### Q63. What is the difference between a YARN container and a Docker container?
**Answer:** A YARN container is a resource allocation (memory + vCores) running a process under cgroups limits on a NodeManager. Docker containers package an application with its dependencies; YARN can launch Docker containers (Hadoop 3) to isolate environments, such as Python versions for Spark jobs.

---

### Q64. What is dominant resource fairness (DRF)?
**Answer:** A scheduling policy that considers multiple resource types (memory and CPU) when sharing, by equalizing each tenant's dominant share (the resource they use proportionally most). In Capacity Scheduler it's enabled using `DominantResourceCalculator`; otherwise only memory is considered.

---

### Q65. What is uber mode?
**Answer:** For very small MapReduce jobs, the AM runs the map and reduce tasks inside its own JVM rather than requesting new containers, avoiding container startup overhead (`mapreduce.job.ubertask.enable`).

---

### Q66. What are NodeManager health checks and blacklisting?
**Answer:** NMs run a health-check script and monitor disk health; unhealthy nodes (bad disks, script failure) are reported to the RM and removed from scheduling. ApplicationMasters can also blacklist nodes where tasks keep failing.

---

### Q67. How does YARN handle data locality?
**Answer:** The AM requests containers with node and rack preferences based on block locations. The scheduler tries node-local, waits a configurable number of scheduling opportunities (delay scheduling), then falls back to rack-local and off-rack so jobs don't stall waiting for the perfect node.

---

### Q68. Scenario: how would you size YARN for a new 20-node cluster, each node with 128 GB RAM and 32 cores?
**Answer:** Reserve resources for the OS and daemons (about 16–24 GB RAM and 2–4 cores), so give YARN roughly 104 GB and 28 vCores per node. Set min allocation 1–2 GB, max close to the node's YARN memory, define queues by workload, and size containers (4–8 GB, 1–2 cores per task). Validate with representative workloads and tune after monitoring queue wait times, memory use, and GC.

---

# SECTION 4: MAPREDUCE

### Q69. Explain the MapReduce programming model.
**Answer:** Input is split into chunks; **map** tasks transform each record into key-value pairs; the framework **shuffles and sorts** pairs so that all values of a key arrive at the same **reduce** task; reducers aggregate and write output to HDFS. Example: word count, where map emits `(word, 1)` and reduce sums the counts.

---

### Q70. Describe the phases of a MapReduce job.
**Answer:** Input splits → RecordReader → Map → (Combiner) → Partitioner → Spill/Sort → Shuffle (copy to reducers) → Merge → Reduce → OutputFormat → HDFS. The shuffle/sort between map and reduce is the most expensive phase.

---

### Q71. InputSplit vs HDFS block?
**Answer:** A block is a physical storage unit (128 MB). A split is a logical chunk of data fed to one mapper, typically equal to a block, but it may differ (a record can span block boundaries, and the RecordReader handles boundaries). The number of mappers equals the number of splits.

---

### Q72. What is a Combiner and when is it safe?
**Answer:** A "mini-reducer" that runs on map output locally to reduce the data shuffled across the network. It must be commutative and associative (sum, max, count work; average does not, unless you carry (sum, count) pairs). The framework may run it zero, one, or several times, so correctness can't depend on it.

---

### Q73. What is a Partitioner?
**Answer:** It decides which reducer receives each key. The default `HashPartitioner` uses `hash(key) % numReducers`. A custom partitioner can fix skew or ensure related keys land together (e.g., partition by date for date-wise outputs).

---

### Q74. How many reducers should you use?
**Answer:** A common guideline is 0.95 or 1.75 × (number of nodes × max containers per node). Too few reducers cause long tails and large outputs; too many create small output files and scheduling overhead. Set with `mapreduce.job.reduces`, and use 0 reducers for map-only jobs (filters, format conversion).

---

### Q75. What is a map-only job and when do you use it?
**Answer:** Setting reducers to 0 so map output is written directly to HDFS, skipping shuffle and sort. Use for filtering, parsing, and format conversion (CSV to Parquet).

---

### Q76. Explain the shuffle and sort in detail.
**Answer:** Map output goes to an in-memory circular buffer (`mapreduce.task.io.sort.mb`, default 100 MB); when about 80% full, it spills to local disk, partitioned and sorted by key (the combiner may run). Spill files are merged into one partitioned file. Reducers fetch their partition from every mapper over HTTP, merge-sort the fetched pieces, and group by key for the reduce function.

---

### Q77. What is speculative execution?
**Answer:** If a task runs much slower than its peers (a "straggler"), the framework launches a duplicate on another node; the first to finish wins and the other is killed. It helps with slow nodes but wastes resources and can cause trouble when tasks have side effects (writing to external databases), so disable it for those jobs.

---

### Q78. How does MapReduce handle task and application failures?
**Answer:** Failed tasks are retried (default 4 attempts for maps/reduces, `mapreduce.map.maxattempts`); repeated failures fail the job. If the AM fails, YARN restarts it (default 2 attempts). If a node fails, its completed map outputs are re-executed because they were stored on its local disk.

---

### Q79. What are counters?
**Answer:** Named, aggregated counters collected from tasks: built-in (bytes read, records, spilled records) and custom ones (malformed rows). They are used for debugging and data-quality metrics, such as counting rejected records.

---

### Q80. What is the distributed cache?
**Answer:** A way to ship read-only files (lookup tables, jars, configs) to every task node, used for map-side joins with small datasets. The files are copied once per node rather than per task.

---

### Q81. Reduce-side join vs map-side join?
**Answer:** Reduce-side join: both datasets are tagged and shuffled by join key, so it works for any size but is expensive. Map-side join: the small table is loaded into memory (distributed cache) in every mapper, so no shuffle is needed, but the small table must fit in memory. Hive's map join and Spark's broadcast join implement the same idea.

---

### Q82. How do you handle data skew in MapReduce?
**Answer:** Salt hot keys (append a random suffix and aggregate in two steps), use a custom partitioner, apply a combiner, pre-aggregate in the mapper, filter or separately process null/default keys, and increase reducers. Detect skew via counters and a few reducers running much longer than the rest.

---

### Q83. Write a WordCount mapper and reducer (Java outline).
**Answer:**
```java
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable ONE = new IntWritable(1);
  private Text word = new Text();
  public void map(LongWritable k, Text v, Context ctx) throws IOException, InterruptedException {
    for (String w : v.toString().split("\\s+")) { word.set(w); ctx.write(word, ONE); }
  }
}
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  public void reduce(Text k, Iterable<IntWritable> vals, Context ctx) throws IOException, InterruptedException {
    int sum = 0; for (IntWritable v : vals) sum += v.get();
    ctx.write(k, new IntWritable(sum));
  }
}
```
Add `job.setCombinerClass(WCReducer.class)` for local aggregation.

---

### Q84. What is Hadoop Streaming?
**Answer:** A utility that lets you write mappers and reducers in any language that reads stdin and writes stdout (Python, shell):
```bash
hadoop jar hadoop-streaming.jar -input /in -output /out -mapper mapper.py -reducer reducer.py -file mapper.py -file reducer.py
```
Handy for quick jobs, though it's slower than native Java due to serialization.

---

### Q85. Why was MapReduce replaced by Tez and Spark for most workloads?
**Answer:** MapReduce writes intermediate results to disk between every stage and starts a new JVM per task, so multi-stage jobs (like a Hive query with several joins) are slow. Tez models a query as a single DAG and keeps data in memory/pipelines between stages; Spark does in-memory processing with a DAG scheduler. MapReduce is still stable for very large, simple batch jobs.

---

# SECTION 5: HIVE ARCHITECTURE & BASICS

### Q86. What is Hive and what is it used for?
**Answer:** Hive is a data warehouse layer on Hadoop that exposes SQL (HiveQL) over files in HDFS/object storage and translates queries into MapReduce, Tez, or Spark jobs. It's designed for large-scale batch analytics (ETL, reporting) and not for low-latency OLTP.

---

### Q87. Explain Hive architecture.
**Answer:**
- **Clients**: Beeline (JDBC to HiveServer2), JDBC/ODBC tools, Hue.
- **HiveServer2**: accepts queries, handles sessions and authentication.
- **Driver/Compiler/Optimizer**: parses SQL, builds the logical plan, optimizes (CBO via Calcite), and generates the physical plan (DAG).
- **Execution engine**: MapReduce, Tez, or Spark runs the plan on YARN.
- **Metastore**: a relational database (MySQL/PostgreSQL) holding table schemas, partitions, and locations.
- **Storage**: HDFS or cloud object storage holding the actual data.

---

### Q88. What is the Hive Metastore and why is it critical?
**Answer:** The central catalog of databases, tables, columns, partitions, file formats, and locations. It's shared by Hive, Spark, Presto/Trino, Impala, and others. If it's down or corrupted, tables can't be resolved even though the data is intact, so it needs backups and HA of the backing RDBMS. Use a remote metastore service, not an embedded Derby database, in production.

---

### Q89. Embedded, local, and remote metastore modes?
**Answer:** Embedded: Derby in the same JVM as Hive (one session only; for demos). Local: metastore code in the Hive process with an external RDBMS. Remote: a separate Metastore service (Thrift) that clients connect to, the standard production setup for sharing and security.

---

### Q90. What is the difference between the Hive CLI and Beeline?
**Answer:** The old CLI talked directly to the metastore/HDFS, bypassing HiveServer2 security. Beeline is a JDBC client that connects to HiveServer2, supporting authentication, authorization (Ranger), and concurrency; the CLI is deprecated.
```bash
beeline -u "jdbc:hive2://hs2-host:10000/default;principal=hive/_HOST@REALM"
```

---

### Q91. Hive vs RDBMS?
**Answer:** Hive: schema-on-read, batch latency (seconds to minutes or longer), large scale, append-oriented (updates via ACID tables), no enforced constraints. RDBMS: schema-on-write, millisecond latency, row-level transactions, enforced constraints and indexes, and smaller scale.

---

### Q92. What are the Hive execution engines?
**Answer:** MapReduce (legacy, deprecated), Tez (default in modern distributions; DAG-based, container reuse), and Spark (Hive on Spark). Set with `set hive.execution.engine=tez;`. LLAP (Live Long and Process) adds persistent daemons with caching for sub-second interactive queries.

---

### Q93. What is HiveServer2?
**Answer:** The service that enables multi-client concurrent access over Thrift/JDBC/ODBC, with Kerberos/LDAP authentication, session management, and integration with Ranger. Applications and BI tools connect through it.

---

### Q94. What are Hive's data types?
**Answer:** Primitive: `TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL(p,s), STRING, VARCHAR, CHAR, BOOLEAN, DATE, TIMESTAMP, BINARY`. Complex: `ARRAY<T>`, `MAP<K,V>`, `STRUCT<...>`, `UNIONTYPE`. Use `DECIMAL` for money rather than `DOUBLE`.

---

### Q95. How do you create a database and see its location?
**Answer:**
```sql
CREATE DATABASE IF NOT EXISTS sales LOCATION '/data/warehouse/sales.db';
DESCRIBE DATABASE EXTENDED sales;
USE sales;
```

---

### Q96. Managed (internal) vs external tables?
**Answer:** Managed table: Hive owns the data; `DROP TABLE` deletes both metadata and data (in Hive 3, managed tables are transactional ACID ORC). External table: Hive owns only metadata; dropping removes the table definition but keeps the files. Use external tables for raw/landing data shared with other tools, and managed (or table-format) tables for curated data.

---

### Q97. Create an external table over CSV files.
**Answer:**
```sql
CREATE EXTERNAL TABLE raw.orders (
  order_id BIGINT, customer_id BIGINT, amount DECIMAL(12,2), order_ts STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/orders/'
TBLPROPERTIES ("skip.header.line.count"="1");
```
For quoted CSV with embedded commas, use `OpenCSVSerde`.

---

### Q98. What is a SerDe?
**Answer:** Serializer/Deserializer: the component that tells Hive how to read a row from file bytes and write it back. Examples: `LazySimpleSerDe` (delimited text), `OpenCSVSerde`, `JsonSerDe`, `RegexSerDe` (log files), plus the ORC, Parquet and Avro SerDes.

---

### Q99. How do you load data into Hive?
**Answer:**
```sql
LOAD DATA LOCAL INPATH '/tmp/orders.csv' INTO TABLE raw.orders;   -- copy from local
LOAD DATA INPATH '/data/in/orders.csv' INTO TABLE raw.orders;      -- MOVES the file within HDFS
INSERT INTO TABLE curated.orders SELECT ... FROM raw.orders;
INSERT OVERWRITE TABLE curated.orders PARTITION (dt='2026-10-25') SELECT ...;
CREATE TABLE t AS SELECT ...;   -- CTAS
```
`LOAD DATA` doesn't transform data; it only moves/copies files, so the format must already match the table.

---

### Q100. `INSERT INTO` vs `INSERT OVERWRITE`?
**Answer:** `INSERT INTO` appends. `INSERT OVERWRITE` replaces the table's data (or the specific partitions being written). `INSERT OVERWRITE` on partitioned tables only replaces partitions present in the written data when using dynamic partitions, which gives you idempotent re-runs per partition.

---

### Q101. What does `DESCRIBE FORMATTED` show?
**Answer:** Columns, partitions, owner, location, table type (MANAGED/EXTERNAL), input/output formats, SerDe, and table properties including statistics.
```sql
DESCRIBE FORMATTED sales.orders;
SHOW PARTITIONS sales.orders;
SHOW CREATE TABLE sales.orders;
```

---

### Q102. How do you change a table (ALTER)?
**Answer:**
```sql
ALTER TABLE t ADD COLUMNS (channel STRING);
ALTER TABLE t RENAME TO t_v2;
ALTER TABLE t SET TBLPROPERTIES ('comment'='...');
ALTER TABLE t ADD PARTITION (dt='2026-10-25') LOCATION '/data/raw/t/dt=2026-10-25';
ALTER TABLE t DROP PARTITION (dt='2025-01-01');
ALTER TABLE t CHANGE COLUMN amt amount DECIMAL(12,2);
```
Adding a column only changes metadata, so old files return NULL for it. For ORC/Parquet, be careful about column order and renames, since they may not map correctly to the file schema.

---

### Q103. What is `TRUNCATE TABLE` vs `DROP TABLE` vs `DELETE`?
**Answer:** `TRUNCATE` removes all data but keeps the table (only allowed on managed tables). `DROP` removes the table definition (and data for managed tables). `DELETE` removes rows and requires an ACID transactional table.

---

### Q104. What are Hive variables and `hiveconf`?
**Answer:** Parameterize scripts: `beeline -f etl.hql --hivevar run_date=2026-10-25` and in the script `WHERE dt = '${hivevar:run_date}'`. `--hiveconf` sets configuration properties. Used heavily in scheduler-driven daily jobs.

---

### Q105. How do you run a Hive script from a scheduler?
**Answer:**
```bash
beeline -u "$HS2_URL" -f /jobs/daily_sales.hql --hivevar run_date=$(date -d yesterday +%F)
```
Check the exit code, capture logs, and make the HQL idempotent (`INSERT OVERWRITE` per partition) so a retry is safe.

---

# SECTION 6: HIVE TABLES, PARTITIONING, BUCKETING, FILE FORMATS

### Q106. What is partitioning in Hive and why use it?
**Answer:** Partitioning splits a table into separate directories by the values of one or more columns (e.g., `.../orders/dt=2026-10-25/`). Queries that filter on the partition column read only the matching directories (**partition pruning**), which drastically cuts I/O. Typical partition columns are date, region, or source system.

---

### Q107. Create a partitioned table and load a partition.
**Answer:**
```sql
CREATE TABLE curated.orders (
  order_id BIGINT, customer_id BIGINT, amount DECIMAL(12,2)
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

INSERT OVERWRITE TABLE curated.orders PARTITION (dt='2026-10-25')
SELECT order_id, customer_id, amount FROM raw.orders WHERE order_date = '2026-10-25';
```
The partition column is not stored inside the data files; it lives in the directory name and the metastore.

---

### Q108. Static vs dynamic partitioning?
**Answer:** Static: you specify the partition value in the statement (`PARTITION (dt='2026-10-25')`). Dynamic: Hive derives partition values from the last columns of the SELECT, creating partitions as needed. Dynamic is convenient for loading many partitions (backfills), static is safer and more predictable for daily loads.
```sql
INSERT OVERWRITE TABLE curated.orders PARTITION (dt)
SELECT order_id, customer_id, amount, order_date AS dt FROM raw.orders;
```

---

### Q109. Which settings control dynamic partitioning?
**Answer:**
```sql
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;      -- strict requires at least one static partition
SET hive.exec.max.dynamic.partitions=5000;           -- total per statement (default 1000)
SET hive.exec.max.dynamic.partitions.pernode=500;    -- per mapper/reducer (default 100)
```
Errors like "Number of dynamic partitions exceeded" mean one of these limits was hit, which often signals bad partition column choice or dirty data (e.g., many malformed date values).

---

### Q110. How do you choose partition columns? What is over-partitioning?
**Answer:** Choose columns that are frequently filtered on and have low-to-moderate cardinality (date, country). Avoid high-cardinality columns (customer_id, timestamp): they create millions of tiny directories and files, overloading the NameNode and metastore and making queries slower. A partition should typically hold at least a few hundred MB of data; if daily partitions are tiny, partition by month instead.

---

### Q111. What does `MSCK REPAIR TABLE` do?
**Answer:** It scans the table's location in HDFS/S3 and registers partition directories (`dt=...`) that exist on storage but not in the metastore. Needed when files are written outside Hive (Spark, distcp, ingestion tools) to an external table.
```sql
MSCK REPAIR TABLE raw.events;
-- or, explicit and cheaper for one partition
ALTER TABLE raw.events ADD IF NOT EXISTS PARTITION (dt='2026-10-25');
```
On tables with a huge number of partitions, MSCK can be slow; adding specific partitions is better.

---

### Q112. Scenario: an upstream job drops files into `/data/raw/events/dt=2026-10-25/` but your Hive query returns no rows for that date. Why?
**Answer:** The partition isn't registered in the metastore. Run `MSCK REPAIR TABLE` or `ALTER TABLE ... ADD PARTITION`. Also verify that the directory naming matches `col=value`, that the file format matches the table definition, that the files aren't hidden (names starting with `_` or `.` are ignored), and that permissions allow reading.

---

### Q113. What is partition pruning and how do you make sure it happens?
**Answer:** Hive's optimizer skips partitions that don't match the filter on the partition column. Ensure the predicate refers to the raw partition column (`WHERE dt = '2026-10-25'`), not a function applied to it (`WHERE substr(dt,1,7) = '2026-10'` may prevent pruning in some versions), and use `EXPLAIN DEPENDENCY` or `EXPLAIN EXTENDED` to confirm which partitions are scanned. In strict mode (`hive.mapred.mode=strict`) queries on partitioned tables must have a partition filter.

---

### Q114. Can you change the partition column of an existing table?
**Answer:** No. Partition columns can't be altered in place. Create a new table with the desired partitioning, load it with `INSERT ... SELECT` (dynamic partitions), validate counts, then swap names. Apache Iceberg's hidden partitioning and partition evolution solve this limitation.

---

### Q115. How do you delete old partitions per retention policy?
**Answer:**
```sql
ALTER TABLE raw.events DROP IF EXISTS PARTITION (dt < '2026-07-01');
```
For external tables, this drops only the metadata, so remove the files separately (`hdfs dfs -rm -r`) or set `external.table.purge=true`. Automate retention in a scheduled job.

---

### Q116. What is bucketing and why use it?
**Answer:** Bucketing hashes a column's value into a fixed number of files (buckets) within a table or partition: `hash(col) % N`. Benefits: more efficient joins (bucket map join / sort-merge-bucket join), efficient sampling, and more even file sizes than high-cardinality partitions.
```sql
CREATE TABLE curated.customers (
  customer_id BIGINT, name STRING, city STRING
)
CLUSTERED BY (customer_id) SORTED BY (customer_id) INTO 32 BUCKETS
STORED AS ORC;
```

---

### Q117. Partitioning vs bucketing?
**Answer:**
| Aspect | Partitioning | Bucketing |
|---|---|---|
| Physical layout | Separate directories | Files inside a table/partition |
| Column cardinality | Low | High |
| Purpose | Prune data scans | Join and sampling efficiency, even distribution |
| Count | Varies with data | Fixed at creation |
| Combined | Yes: partition by date, bucket by customer_id | |

---

### Q118. How do you optimize joins with bucketing?
**Answer:** If both tables are bucketed (and sorted) on the join key with compatible bucket counts (equal or multiples), Hive can join corresponding buckets directly without a full shuffle.
```sql
SET hive.optimize.bucketmapjoin=true;
SET hive.auto.convert.sortmerge.join=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;
```
The data must really be bucketed correctly by Hive (loaded through `INSERT`, not just copied files).

---

### Q119. How do you sample data in Hive?
**Answer:**
```sql
SELECT * FROM curated.customers TABLESAMPLE (BUCKET 1 OUT OF 32 ON customer_id);
SELECT * FROM curated.orders TABLESAMPLE (1 PERCENT);
```
Bucket sampling on a bucketed table reads only the relevant bucket files.

---

### Q120. What are skewed tables?
**Answer:** `CREATE TABLE ... SKEWED BY (col) ON (hot_values) [STORED AS DIRECTORIES]` tells Hive that certain values are very frequent, so it can store and process them separately to reduce skew. In practice, runtime skew handling (`hive.optimize.skewjoin`) and AQE-like features in the engine are more commonly used.

---

### Q121. Compare file formats in Hive.
**Answer:**
| Format | Layout | Notes |
|---|---|---|
| TextFile | Row | Simple, human-readable, slow, no schema or types |
| SequenceFile | Row, binary | Key-value, splittable with compression, legacy |
| Avro | Row, binary | Embedded schema, great schema evolution, good for ingestion |
| RCFile | Hybrid columnar | Legacy, superseded by ORC |
| ORC | Columnar | Hive-optimized, ACID support, indexes, bloom filters |
| Parquet | Columnar | Best cross-engine support (Spark, Impala, Presto) |

---

### Q122. Why is ORC faster?
**Answer:** Columnar storage reads only the needed columns; data is compressed per column; stripes carry min/max statistics and a row index (every 10,000 rows) enabling predicate pushdown to skip stripes and row groups; optional bloom filters speed up point lookups; and it works with vectorized execution. Typical size reductions are 70–90% versus text.

---

### Q123. ORC vs Parquet: how do you choose?
**Answer:** ORC: best inside the Hive/Tez/LLAP world, supports ACID tables natively, and has strong built-in indexes. Parquet: the de facto standard across Spark, Impala, Trino, Athena, and Snowflake, with good nested-data support. If the lake is Spark-centric or multi-engine, choose Parquet; if it's primarily Hive with transactional tables, choose ORC.

---

### Q124. Convert a text-based raw table into ORC (typical ETL step).
**Answer:**
```sql
CREATE TABLE curated.events (
  event_id STRING, user_id BIGINT, event_type STRING, event_ts TIMESTAMP
)
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE curated.events PARTITION (dt)
SELECT event_id, CAST(user_id AS BIGINT), event_type,
       CAST(event_ts AS TIMESTAMP), to_date(event_ts) AS dt
FROM raw.events_text;
```
Validate row counts and spot-check aggregates between the raw and curated tables.

---

### Q125. How do you set compression in Hive?
**Answer:**
```sql
SET hive.exec.compress.output=true;                        -- final output
SET hive.exec.compress.intermediate=true;                  -- between stages
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
```
For ORC use the table property `orc.compress` (ZLIB default, SNAPPY, ZSTD in newer versions); for Parquet use `parquet.compression`. Snappy favors speed; ZLIB/ZSTD favor smaller size.

---

### Q126. How do you handle schema evolution in Hive tables?
**Answer:** Adding columns at the end is safe for most formats (old data returns NULL). Avro has the most robust evolution via the reader/writer schema; ORC and Parquet map columns by name or position depending on settings (`orc.force.positional.evolution`, `parquet.column.index.access`), so renames and reordering can silently misalign data. Use additive-only changes, test on a copy, and consider Iceberg for full-featured evolution.

---

### Q127. How do you query JSON data in Hive?
**Answer:** Use a JSON SerDe for one-JSON-per-line files, or functions on a raw string column.
```sql
CREATE EXTERNAL TABLE raw.logs (
  user_id BIGINT, event STRING, props MAP<STRING,STRING>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/data/raw/logs/';

SELECT get_json_object(line, '$.user.id') AS uid,
       json_tuple(line, 'event', 'ts') AS (event, ts)
FROM raw.logs_string;
```
`get_json_object` is flexible but slow on large data, so parse once and store typed columns in ORC.

---

### Q128. Working with complex types: ARRAY, MAP, STRUCT.
**Answer:**
```sql
CREATE TABLE t (id INT, tags ARRAY<STRING>, attrs MAP<STRING,STRING>,
                addr STRUCT<city:STRING, zip:STRING>);
SELECT tags[0], attrs['color'], addr.city, size(tags) FROM t;
```
They're useful for nested JSON/Avro/Parquet sources without flattening everything at ingestion.

---

### Q129. What does `LATERAL VIEW explode` do?
**Answer:** Turns each element of an array/map into its own row and joins it with the original row.
```sql
SELECT o.order_id, item
FROM orders o
LATERAL VIEW explode(o.items) t AS item;
-- keep rows with empty arrays
LATERAL VIEW OUTER explode(o.items) t AS item
```
`posexplode` also returns the element's position.

---

### Q130. Views vs materialized views?
**Answer:** A view is a stored query, evaluated each time (no data stored), useful for abstraction and security (hide columns). A materialized view (Hive 3) stores precomputed results and the optimizer can transparently rewrite queries to use it, with rebuild/refresh via `ALTER MATERIALIZED VIEW ... REBUILD`. It suits heavy repeated aggregations.

---

### Q131. What are temporary tables and CTEs in Hive?
**Answer:** `CREATE TEMPORARY TABLE` exists only for the session (stored in the user's scratch directory) and is handy for intermediate steps. A CTE (`WITH x AS (...)`) names a subquery for readability; in Hive, CTEs are inlined (re-evaluated for every reference) unless materialization is enabled (`hive.optimize.cte.materialize.threshold`), so a CTE referenced multiple times may be computed multiple times.

---

# SECTION 7: HIVEQL — JOINS, FUNCTIONS, WINDOW FUNCTIONS & QUERY SCENARIOS

### Q132. Explain `ORDER BY`, `SORT BY`, `DISTRIBUTE BY`, and `CLUSTER BY`.
**Answer:**
- `ORDER BY`: total global ordering via a single reducer (slow on big data; Hive strict mode requires `LIMIT`).
- `SORT BY`: sorts within each reducer only (no global order).
- `DISTRIBUTE BY`: controls which reducer gets which rows (by hash of the column).
- `CLUSTER BY col`: shorthand for `DISTRIBUTE BY col SORT BY col`.

`DISTRIBUTE BY ... SORT BY ...` is commonly used to write well-clustered, sorted ORC files.

---

### Q133. Explain `GROUP BY`, `HAVING`, `GROUPING SETS`, `ROLLUP`, and `CUBE`.
**Answer:**
```sql
SELECT region, product, SUM(amount)
FROM sales GROUP BY region, product WITH ROLLUP;       -- subtotals and grand total
SELECT region, product, SUM(amount)
FROM sales GROUP BY region, product GROUPING SETS ((region, product), (region), ());
```
`HAVING` filters groups after aggregation, `WHERE` filters rows before. `CUBE` produces all combinations of subtotals.

---

### Q134. Tell me about the types of joins in Hive.
**Answer:** `INNER`, `LEFT/RIGHT/FULL OUTER`, `CROSS`, and `LEFT SEMI JOIN` (like `EXISTS`; returns only left-side columns). Equi-joins are the norm; non-equi conditions are supported in newer versions but can be expensive. Anti-join pattern: `LEFT JOIN ... WHERE b.key IS NULL`.

---

### Q135. What is a LEFT SEMI JOIN and when is it better than `IN`/`EXISTS`/inner join?
**Answer:**
```sql
SELECT c.* FROM customers c
LEFT SEMI JOIN orders o ON c.customer_id = o.customer_id;
```
Returns each customer at most once if any order exists, with no duplicates (an inner join would multiply rows by order count) and without shuffling right-side columns. Right-side columns can only be referenced in the `ON` clause.

---

### Q136. Explain map-side (broadcast) join.
**Answer:** The small table is loaded into memory in each mapper, so the big table doesn't need a shuffle.
```sql
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask.size=268435456;   -- ~256 MB, size for combined small tables
SELECT /*+ MAPJOIN(d) */ f.*, d.name FROM fact f JOIN dim d ON f.dim_id = d.id;
```
The default `hive.mapjoin.smalltable.filesize` is 25 MB; with Tez the noconditionaltask size is the main control.

---

### Q137. What is a skew join and how do you enable it?
**Answer:** When one join key has far more rows than others, one reducer becomes a bottleneck. Hive can handle it by processing the hot keys in a separate map join.
```sql
SET hive.optimize.skewjoin=true;
SET hive.skewjoin.key=100000;     -- rows per key threshold to consider it skewed
```
Alternatives: filter/handle NULL keys separately, salt the key, or broadcast the smaller side.

---

### Q138. What is a sort-merge-bucket (SMB) join?
**Answer:** When both tables are bucketed and sorted on the join key into the same (or multiple) number of buckets, mappers merge-join matching bucket files without any shuffle or hash tables, which works for large-large joins that can't be broadcast.

---

### Q139. `UNION` vs `UNION ALL`?
**Answer:** `UNION` removes duplicates (an extra shuffle/distinct step); `UNION ALL` just concatenates. Use `UNION ALL` unless you need deduplication; it is much cheaper.

---

### Q140. How do NULLs behave in joins and aggregates?
**Answer:** `NULL = NULL` is not true, so NULL keys never match in equi-joins. `COUNT(*)` counts rows, `COUNT(col)` ignores NULLs, `SUM/AVG` ignore NULLs, and NULL keys all land on one reducer in `GROUP BY`/`DISTINCT` (a classic skew source). Handle with `COALESCE` or by filtering and processing NULL keys separately.

---

### Q141. Conditional functions: `CASE`, `IF`, `COALESCE`, `NVL`.
**Answer:**
```sql
SELECT CASE WHEN amount >= 10000 THEN 'HIGH' WHEN amount >= 1000 THEN 'MID' ELSE 'LOW' END AS band,
       IF(status='PAID', 1, 0) AS is_paid,
       COALESCE(city, state, 'UNKNOWN') AS loc,
       NVL(discount, 0) AS discount
FROM orders;
```

---

### Q142. Useful string functions and regex in Hive?
**Answer:** `concat`, `concat_ws`, `substr`, `trim`, `lower`, `upper`, `split`, `regexp_replace`, `regexp_extract`, `instr`, `length`, `lpad`, `rpad`.
```sql
SELECT regexp_extract(url, 'utm_source=([^&]+)', 1) AS source,
       regexp_replace(phone, '[^0-9]', '') AS phone_digits
FROM clicks;
```

---

### Q143. Date and time functions?
**Answer:** `current_date`, `current_timestamp`, `to_date`, `date_add/date_sub`, `datediff`, `add_months`, `trunc`, `last_day`, `year/month/day`, `from_unixtime`, `unix_timestamp`, `date_format`.
```sql
SELECT datediff(current_date, to_date(order_ts)) AS age_days,
       date_format(order_ts, 'yyyy-MM') AS ym,
       trunc(order_ts, 'MM') AS month_start
FROM orders;
```
Be careful with time zones: `from_unixtime` uses the server's time zone.

---

### Q144. Aggregation helpers: `collect_set`, `collect_list`, `concat_ws`.
**Answer:**
```sql
SELECT customer_id, concat_ws(',', collect_set(product)) AS products
FROM purchases GROUP BY customer_id;
```
`collect_set` removes duplicates; `collect_list` keeps them. Both build in-memory arrays per group, so a huge group can cause out-of-memory errors.

---

### Q145. What are window functions in Hive? Give the main ones.
**Answer:** They compute a value for each row over a related set of rows without collapsing them: ranking (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`), offset (`LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`), and aggregates over frames (`SUM() OVER (...)`).
```sql
SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_ts
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
```

---

### Q146. Scenario: get the latest record per customer from a table with multiple versions.
**Answer:**
```sql
SELECT * FROM (
  SELECT t.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
  FROM customers_history t
) x WHERE rn = 1;
```
Use `ROW_NUMBER` (not `RANK`) when exactly one row per key is required, and add a deterministic tie-breaker to `ORDER BY`.

---

### Q147. Scenario: find the second (nth) highest salary in each department.
**Answer:**
```sql
SELECT dept, emp, salary FROM (
  SELECT dept, emp, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rk
  FROM employees
) t WHERE rk = 2;
```
`DENSE_RANK` handles ties without skipping ranks; `RANK` would skip numbers after ties.

---

### Q148. Scenario: remove duplicate rows from a Hive table.
**Answer:**
```sql
CREATE TABLE curated.orders_dedup AS
SELECT order_id, customer_id, amount, updated_at FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) rn
  FROM curated.orders
) x WHERE rn = 1;
```
Validate counts, then swap tables (or `INSERT OVERWRITE` partition by partition to avoid rewriting everything). `SELECT DISTINCT` only works when entire rows are identical.

---

### Q149. Scenario: running total and previous-order comparison per customer.
**Answer:**
```sql
SELECT customer_id, order_ts, amount,
       SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_ts) AS running_total,
       LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_ts) AS prev_amount,
       amount - LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_ts) AS diff
FROM orders;
```

---

### Q150. Scenario: top 3 products by revenue in each category.
**Answer:**
```sql
SELECT * FROM (
  SELECT category, product, SUM(amount) AS revenue,
         ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(amount) DESC) AS rn
  FROM sales GROUP BY category, product
) t WHERE rn <= 3;
```
Hive allows window functions over aggregates in the same SELECT, evaluated after `GROUP BY`.

---

### Q151. Scenario: pivot rows to columns (monthly revenue per product).
**Answer:** Hive has no `PIVOT` keyword, so use conditional aggregation.
```sql
SELECT product,
       SUM(CASE WHEN month = '2026-08' THEN amount ELSE 0 END) AS aug,
       SUM(CASE WHEN month = '2026-09' THEN amount ELSE 0 END) AS sep,
       SUM(CASE WHEN month = '2026-10' THEN amount ELSE 0 END) AS oct
FROM monthly_sales GROUP BY product;
```
The reverse (unpivot) uses `LATERAL VIEW explode(map(...))` or `UNION ALL`.

---

### Q152. Scenario: find users who logged in on 3 or more consecutive days (gaps and islands).
**Answer:**
```sql
SELECT user_id, MIN(dt) AS streak_start, MAX(dt) AS streak_end, COUNT(*) AS days
FROM (
  SELECT user_id, dt,
         date_sub(dt, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt)) AS grp
  FROM (SELECT DISTINCT user_id, to_date(login_ts) AS dt FROM logins) d
) t
GROUP BY user_id, grp
HAVING COUNT(*) >= 3;
```
Consecutive dates minus an increasing row number give a constant value per streak. Deduplicate days first.

---

### Q153. Scenario: sessionize clickstream events (new session after 30 minutes of inactivity).
**Answer:**
```sql
WITH x AS (
  SELECT user_id, event_ts,
         unix_timestamp(event_ts) - unix_timestamp(LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts)) AS gap
  FROM events
),
y AS (
  SELECT *, CASE WHEN gap IS NULL OR gap > 1800 THEN 1 ELSE 0 END AS new_session FROM x
)
SELECT user_id, event_ts,
       SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_ts) AS session_id
FROM y;
```

---

### Q154. Scenario: customers who purchased in January but not in February.
**Answer:**
```sql
SELECT DISTINCT j.customer_id
FROM (SELECT customer_id FROM orders WHERE month = '2026-01') j
LEFT JOIN (SELECT customer_id FROM orders WHERE month = '2026-02') f
  ON j.customer_id = f.customer_id
WHERE f.customer_id IS NULL;
```

---

### Q155. Scenario: calculate month-over-month growth in revenue.
**Answer:**
```sql
SELECT month, revenue,
       ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0
             / LAG(revenue) OVER (ORDER BY month), 2) AS mom_pct
FROM (SELECT date_format(order_ts, 'yyyy-MM') AS month, SUM(amount) AS revenue
      FROM orders GROUP BY date_format(order_ts, 'yyyy-MM')) m;
```
Division by a zero or NULL previous value yields NULL; wrap with `COALESCE` or a `CASE` if needed.

---

### Q156. Scenario: find orders whose amount is above that customer's own average.
**Answer:**
```sql
SELECT * FROM (
  SELECT o.*, AVG(amount) OVER (PARTITION BY customer_id) AS cust_avg
  FROM orders o
) t WHERE amount > cust_avg;
```
Using a window function avoids a separate aggregate subquery plus a self-join.

---

### Q157. How do you compute median or percentiles in Hive?
**Answer:**
```sql
SELECT percentile_approx(amount, 0.5) AS approx_median,
       percentile_approx(amount, array(0.25, 0.75, 0.95)) AS quartiles
FROM orders;
SELECT percentile(CAST(amount AS BIGINT), 0.5) FROM orders;   -- exact, integers only
```
`percentile_approx` is the practical choice on large data; exact `percentile` requires integer input and is memory-heavy.

---

### Q158. Scenario: find employees who earn more than their manager (self join).
**Answer:**
```sql
SELECT e.emp_name, e.salary, m.emp_name AS manager, m.salary AS mgr_salary
FROM employees e
JOIN employees m ON e.manager_id = m.emp_id
WHERE e.salary > m.salary;
```

---

### Q159. How do you "update" or "delete" rows in a non-transactional Hive table?
**Answer:** Rewrite the data: build the corrected result with `INSERT OVERWRITE` (for the affected partition) using `SELECT` with `CASE` for updates and a `WHERE` that excludes deleted rows.
```sql
INSERT OVERWRITE TABLE curated.orders PARTITION (dt='2026-10-20')
SELECT order_id, customer_id,
       CASE WHEN order_id = 1001 THEN 2500.00 ELSE amount END AS amount
FROM curated.orders
WHERE dt = '2026-10-20' AND order_id <> 1002;
```
For frequent row-level changes use ACID tables or a table format like Iceberg/Delta.

---

### Q160. Scenario: write a quick data-profiling query (row count, null counts, distinct counts, min/max).
**Answer:**
```sql
SELECT COUNT(*) AS total_rows,
       SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer,
       COUNT(DISTINCT order_id) AS distinct_orders,
       MIN(order_ts) AS min_ts, MAX(order_ts) AS max_ts,
       MIN(amount) AS min_amt, MAX(amount) AS max_amt
FROM curated.orders WHERE dt = '2026-10-25';
```
Compare `total_rows` and `distinct_orders` to detect duplicates, and compare to the source count to detect missing data.

---

# SECTION 8: HIVE PERFORMANCE TUNING

### Q161. What is your general checklist for tuning a slow Hive query?
**Answer:**
1. Use Tez (or Spark) rather than MapReduce.
2. Use ORC/Parquet with compression, not text.
3. Partition and filter on the partition column; select only the needed columns.
4. Collect statistics and enable the CBO.
5. Enable vectorization and map-side aggregation.
6. Use map joins for small tables; fix skew.
7. Right-size mappers/reducers and containers; avoid small files.
8. Read the `EXPLAIN` plan and the Tez UI for the slowest vertex.

---

### Q162. How do you run Hive on Tez and tune it?
**Answer:**
```sql
SET hive.execution.engine=tez;
SET hive.tez.container.size=4096;          -- MB
SET hive.tez.java.opts=-Xmx3276m;          -- ~80% of the container
SET tez.queue.name=etl;
```
Tez runs a query as one DAG, reuses containers, and avoids writing intermediate results to HDFS between stages, which usually makes it 2–10x faster than MapReduce.

---

### Q163. What is vectorization?
**Answer:** Processing batches of 1,024 rows at a time with tight loops instead of row by row, improving CPU efficiency.
```sql
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
```
It works best with ORC (and Parquet in newer versions); unsupported types or UDFs make parts of the query fall back to row mode.

---

### Q164. What is the Cost-Based Optimizer (CBO) and how do you feed it?
**Answer:** The CBO (Apache Calcite) uses table and column statistics to choose join order, join type, and so on.
```sql
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
ANALYZE TABLE curated.orders PARTITION (dt='2026-10-25') COMPUTE STATISTICS;
ANALYZE TABLE curated.orders PARTITION (dt='2026-10-25') COMPUTE STATISTICS FOR COLUMNS;
```
Missing or stale statistics are a common reason for poor plans, such as a big table being picked as the build side.

---

### Q165. What is predicate pushdown?
**Answer:** Pushing filters down to the storage layer so ORC/Parquet readers skip stripes or row groups using min/max statistics and bloom filters (`hive.optimize.ppd=true`, `hive.optimize.index.filter=true`). Sorting data on commonly filtered columns when writing (`DISTRIBUTE BY ... SORT BY`) tightens min/max ranges and improves skipping.

---

### Q166. How is the number of reducers decided and how do you control it?
**Answer:** Hive estimates it from the input size: `hive.exec.reducers.bytes.per.reducer` (default 256 MB in modern versions), capped by `hive.exec.reducers.max`. Override with `SET mapreduce.job.reduces=N;`. Too few reducers cause skew and slow tasks; too many create small output files.

---

### Q167. How do you control the number of mappers?
**Answer:** Mappers follow input splits. Tune split sizes with `mapreduce.input.fileinputformat.split.maxsize/minsize`; on Tez the grouping settings `tez.grouping.min-size` and `tez.grouping.max-size` decide how many splits are combined per task. Many tiny files produce many mappers, so combine them (`hive.input.format=CombineHiveInputFormat`, the default).

---

### Q168. How do you reduce small output files from Hive jobs?
**Answer:**
```sql
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.tezfiles=true;
SET hive.merge.smallfiles.avgsize=134217728;    -- merge if the average output file size is below 128 MB
SET hive.merge.size.per.task=268435456;         -- target merged size 256 MB
```
Also use `DISTRIBUTE BY` partition column to control reducer fan-out, or `ALTER TABLE t PARTITION (dt='...') CONCATENATE;` for ORC compaction.

---

### Q169. How do you read an `EXPLAIN` plan?
**Answer:** `EXPLAIN` shows the stages/vertices (Map, Reducer), operators (TableScan, Filter, Select, Group By, Map Join, Reduce Output), and join types. Check: Is the partition filter applied (number of partitions scanned)? Is a Map Join used for small tables? Are there many reduce stages? Are statistics reported (`Statistics: Num rows`)? Use `EXPLAIN EXTENDED` for paths/partitions and `EXPLAIN ANALYZE` to compare estimated and actual rows.

---

### Q170. Scenario: a Tez task fails with `OutOfMemoryError: Java heap space` or "Container killed by YARN". What do you do?
**Answer:**
- Increase `hive.tez.container.size` and set `hive.tez.java.opts` to about 80% of it.
- Check skew: one task handling much more data than others; fix the skew or increase parallelism (lower `hive.exec.reducers.bytes.per.reducer`).
- Reduce the map-join threshold if a hash table is too large (`hive.auto.convert.join.noconditionaltask.size`).
- Avoid heavy `collect_list`/`collect_set` on huge groups.
- Increase `tez.runtime.io.sort.mb` carefully, only within the container limit.

---

### Q171. What is LLAP?
**Answer:** Live Long and Process: long-running daemons on the cluster that cache data and metadata in memory and execute query fragments, so interactive queries skip container startup and repeated disk reads. It brings sub-second to a few seconds' latency for BI-style queries, and also enforces fine-grained security (Ranger) at the daemon level.

---

### Q172. How do you handle skew in `GROUP BY`?
**Answer:**
```sql
SET hive.groupby.skewindata=true;    -- two-stage aggregation: random distribution first, then by key
SET hive.map.aggr=true;              -- partial aggregation on the map side
```
Alternatively, salt manually: aggregate on `(key, FLOOR(RAND()*10))` first, then re-aggregate on `key`. Always check NULL and default-value keys, which commonly concentrate rows.

---

### Q173. Why is `SELECT *` and `COUNT(DISTINCT)` costly?
**Answer:** With columnar formats, `SELECT *` forces reading every column, destroying the main benefit of ORC/Parquet. `COUNT(DISTINCT col)` can funnel all distinct values to one reducer; rewrite as a `GROUP BY` followed by `COUNT(*)`, or use approximate methods when exactness isn't required.
```sql
SELECT COUNT(*) FROM (SELECT customer_id FROM orders GROUP BY customer_id) t;
```

---

### Q174. Scenario: a daily Hive report that took 20 minutes now takes 3 hours. How do you troubleshoot?
**Answer:**
1. Compare with a previous run: input size, number of partitions, and file counts.
2. Check the YARN queue (waiting for resources?) and cluster load.
3. Look at the Tez UI/logs for the slowest vertex; check for skew (one long task) or spill.
4. Review `EXPLAIN` for a changed plan (lost partition pruning, lost map join due to stale or missing stats).
5. Look for small-file explosions in the source tables.
6. Re-run `ANALYZE TABLE`, compact files, fix skew or join strategy, and add runtime alerts.

---

### Q175. Which join-related settings do you commonly adjust?
**Answer:**
```sql
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=536870912;  -- 512 MB total of small tables
SET hive.optimize.skewjoin=true;
SET hive.optimize.bucketmapjoin=true;
```
Put the largest table last in the `JOIN` list (or use `/*+ STREAMTABLE(big) */`) on older versions where the last table is streamed.

---

# SECTION 9: HIVE ACID, UDFs, SECURITY & ECOSYSTEM

### Q176. How do Hive ACID (transactional) tables work?
**Answer:** Hive supports `INSERT`, `UPDATE`, `DELETE`, and `MERGE` on managed ORC tables with `transactional=true`. Writes create immutable **base** and **delta** (and delete-delta) directories; readers merge them at query time using snapshot isolation; the Metastore tracks transactions and locks. In Hive 3, managed tables are ACID by default and bucketing is no longer required.
```sql
CREATE TABLE curated.customers (id BIGINT, name STRING, city STRING)
STORED AS ORC TBLPROPERTIES ('transactional'='true');
UPDATE curated.customers SET city='Pune' WHERE id=101;
```

---

### Q177. What is compaction in ACID tables, and why is it needed?
**Answer:** Each write adds delta files, and too many deltas slow reads. **Minor compaction** merges deltas into larger delta files; **major compaction** rewrites base + deltas into a new base. The compactor runs automatically in the metastore (when enabled) or manually: `ALTER TABLE t PARTITION (dt='...') COMPACT 'major';`. Monitor with `SHOW COMPACTIONS;`.

---

### Q178. Implement an upsert using `MERGE`.
**Answer:**
```sql
MERGE INTO curated.customers AS t
USING staging.customers_delta AS s
ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET name = s.name, city = s.city
WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT VALUES (s.id, s.name, s.city);
```
Deduplicate the delta to the latest row per key first, because multiple source rows matching one target row cause an error.

---

### Q179. Implement SCD Type 2 in Hive (outline).
**Answer:** Keep `start_date`, `end_date`, `is_current`, and an attribute hash. Stage the changes twice: once with the real key (to close the old row) and once with a NULL merge key (to insert the new version).
```sql
MERGE INTO dim_customer t
USING (
  SELECT s.customer_id AS merge_key, s.* FROM stg_customer s
  UNION ALL
  SELECT NULL AS merge_key, s.* FROM stg_customer s
  JOIN dim_customer d ON s.customer_id = d.customer_id AND d.is_current = true
  WHERE s.attr_hash <> d.attr_hash
) src
ON t.customer_id = src.merge_key AND t.is_current = true
WHEN MATCHED AND t.attr_hash <> src.attr_hash THEN
  UPDATE SET is_current = false, end_date = current_date
WHEN NOT MATCHED THEN
  INSERT VALUES (src.customer_id, src.name, src.city, src.attr_hash, current_date, DATE '9999-12-31', true);
```
Exact column lists depend on your table; test with change, new, and unchanged rows.

---

### Q180. What are UDF, UDAF, and UDTF?
**Answer:** UDF: one row in, one value out (`upper`-like). UDAF: aggregates many rows into one (`sum`-like). UDTF: one row in, many rows out (`explode`-like). Write in Java by extending `GenericUDF` (or the simple `UDF`), then register:
```sql
ADD JAR hdfs:///udf/my-udfs.jar;
CREATE FUNCTION mydb.mask_pan AS 'com.company.udf.MaskPan' USING JAR 'hdfs:///udf/my-udfs.jar';
SELECT mydb.mask_pan(card_no) FROM payments;
```
Prefer built-in functions (they're optimized and vectorized); Python scripts through `TRANSFORM` are an option but are slow.

---

### Q181. How is Hive secured in production?
**Answer:** Kerberos (or LDAP) authentication to HiveServer2, authorization with Apache Ranger (database/table/column policies, row filtering, column masking, and audit; Sentry in older stacks), HDFS permissions/ACLs for storage-based authorization, TLS on connections, HDFS encryption zones for sensitive data, and `doAs` or impersonation settings to control which identity accesses HDFS.

---

### Q182. How do Spark and Hive work together?
**Answer:** Spark can use the Hive Metastore as its catalog (`SparkSession.builder.enableHiveSupport()`), so both engines see the same tables.
```python
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
df = spark.sql("select * from curated.orders where dt='2026-10-25'")
df.write.mode("overwrite").insertInto("curated.orders_summary")
```
With Hive 3 ACID managed tables, use the Hive Warehouse Connector (or external tables / table formats like Iceberg), because plain Spark can't read or write full ACID semantics natively.

---

### Q183. What is Sqoop? Show an import and an export.
**Answer:** A tool for bulk transfer between relational databases and Hadoop, using MapReduce for parallelism.
```bash
sqoop import --connect jdbc:mysql://db/sales --username etl --password-file /user/etl/.pw \
  --table orders --target-dir /data/raw/orders/dt=2026-10-25 \
  --split-by order_id -m 8 --as-parquetfile
sqoop export --connect jdbc:mysql://db/sales --table daily_summary \
  --export-dir /data/out/daily_summary -m 4 --staging-table daily_summary_stg --clear-staging-table
```
Sqoop was retired to the Apache Attic in 2021, so new projects use Spark JDBC, NiFi, or CDC tools (Debezium) instead, but it still exists in many legacy stacks.

---

### Q184. Sqoop incremental imports and common pitfalls?
**Answer:**
```bash
sqoop import --connect ... --table orders \
  --incremental lastmodified --check-column updated_at --last-value "2026-10-24 00:00:00" \
  --merge-key order_id --target-dir /data/raw/orders
```
Modes: `append` (new rows by increasing id) and `lastmodified` (changed rows by timestamp; needs `--merge-key` or `--append`). Pitfalls: choose a well-distributed `--split-by` column (skewed or non-numeric keys give uneven mappers), too many mappers overload the source DB, NULLs in the split column are missed, and `--last-value` must be tracked reliably (use a saved job or an external control table).

---

### Q185. Oozie vs Airflow?
**Answer:** Oozie: Hadoop-native workflow scheduler using XML workflows/coordinators, tightly integrated with HDFS/Hive/Sqoop/Spark actions, but limited flexibility and an aging ecosystem. Airflow: Python-defined DAGs, richer operators and UI, retries, sensors, and strong community, working across on-prem and cloud. Most teams migrate Oozie workflows to Airflow.

---

### Q186. Hive vs HBase?
**Answer:** Hive is a SQL warehouse for batch analytics over files (high throughput, higher latency). HBase is a NoSQL, column-family store on HDFS for random real-time reads/writes by row key (low latency). They can be combined: Hive external tables can query HBase via the HBase storage handler, but for analytics, copying data into ORC/Parquet is usually faster.

---

### Q187. Hive vs Impala vs Presto/Trino vs Spark SQL?
**Answer:** Hive: robust for long ETL, fault tolerant, broad format support. Impala: MPP engine for low-latency BI on Hadoop data (memory-bound, not fault tolerant per query). Presto/Trino: federated interactive SQL across many sources. Spark SQL: fast general-purpose ETL plus ML with in-memory processing. All of them can share the Hive Metastore, so tables defined once are queryable from several engines.

---

### Q188. How do you ingest streaming data (Kafka/Flume) into Hive?
**Answer:** Land data in HDFS/object storage in micro-batches, then expose it to Hive: Kafka Connect HDFS sink, Flume HDFS sink, or Spark Structured Streaming writing partitioned Parquet/ORC. Register new partitions (`ALTER TABLE ADD PARTITION` or the connector's Hive integration), compact small files regularly, and keep ingestion idempotent (offset tracking, deduplicate on event id). Hive's streaming ingest API for ACID tables is also available but less commonly used.

---

# SECTION 10: REAL INDUSTRY SCENARIOS

### Q189. Scenario: design a daily batch pipeline loading data from an RDBMS into Hive for reporting.
**Answer:**
1. **Extract**: Sqoop/Spark JDBC incremental pull (`updated_at` watermark) into a raw HDFS path partitioned by load date; keep raw immutable.
2. **Raw layer**: external Hive table over raw files (text/Avro/Parquet).
3. **Curated layer**: `INSERT OVERWRITE` into partitioned ORC tables with typed columns, deduplication (`ROW_NUMBER`), and validation.
4. **Marts**: aggregated tables/materialized views for BI.
5. **Orchestration**: Airflow/Oozie with retries, dependency checks, and SLA alerts.
6. **Quality and ops**: row-count and sum reconciliation against the source, `ANALYZE TABLE`, small-file compaction, partition retention, and monitoring.

---

### Q190. Scenario: a rerun of a failed job created duplicate rows in the target table. How do you prevent this?
**Answer:** Make loads idempotent: write with `INSERT OVERWRITE ... PARTITION (dt='run_date')` rather than `INSERT INTO`, or `MERGE` on a business key for ACID tables; stage output in a temp location and swap on success; keep a control table of completed batch IDs; and dedupe on read (`ROW_NUMBER`) as a safety net. Remove the duplicates by rewriting the affected partition from the deduplicated result.

---

### Q191. Scenario: how do you handle late-arriving data in a date-partitioned table?
**Answer:** Partition by event date but process by arrival: each run reprocesses a rolling window (e.g., the last 3 days) with `INSERT OVERWRITE` for those partitions, tracks `ingest_ts` for audit, and downstream aggregates recompute the affected dates only. Define and communicate a data-completeness SLA (e.g., T+3 days is final). For very late data, a periodic backfill job corrects older partitions.

---

### Q192. Scenario: a Hive job on a 5 TB table has one reducer running for hours while the other 999 finish in minutes.
**Answer:** Classic skew. Find the hot key (`SELECT key, COUNT(*) ... GROUP BY key ORDER BY 2 DESC LIMIT 20`); commonly NULLs, default IDs (0, -1, 'UNKNOWN'), or one huge customer. Fixes: filter or separately process NULL/default keys, enable `hive.optimize.skewjoin` or `hive.groupby.skewindata`, use a map join if one side is small, salt the key and aggregate in two steps, or raise the reducer count. Verify afterwards by comparing the task time distribution.

---

### Q193. Scenario: row counts in Hive don't match the source system. How do you investigate?
**Answer:**
1. Compare counts per partition/date to find which days differ.
2. Check the extraction logic: watermark overlaps/gaps, time zone shifts (UTC vs local day boundaries), rows updated after extraction, or deletes not captured.
3. Check Hive-side issues: duplicates, rows dropped by bad parse (SerDe returned NULLs, malformed lines skipped), partitions not registered, and the filters in the transformation.
4. Compare checksums or sums of key columns, then sample specific IDs on both sides.
5. Fix the root cause, reload the affected partitions, and add an automated reconciliation check that fails the job on a mismatch.

---

### Q194. Scenario: the raw landing zone has 2 million small files per day. What is your remediation plan?
**Answer:** Short term: run a compaction job that reads each day's data and rewrites it into a few hundred large ORC/Parquet files (`INSERT OVERWRITE` with `DISTRIBUTE BY`, or Spark `coalesce`), then delete the originals (after validation). Make downstream jobs read the compacted layer. Long term: fix the producer (batching, bigger roll sizes, message-queue buffering), reduce partition granularity, set HDFS file-count quotas and alerts, and monitor average file size per partition.

---

### Q195. Scenario: an analyst's query `SELECT * FROM events` on a huge table brings the cluster to a crawl. How do you prevent this class of issue?
**Answer:** Set Hive strict mode (`hive.mapred.mode=strict`: require partition filters and `LIMIT` with `ORDER BY`), put ad hoc users into a capped YARN queue with per-user limits, use Ranger policies and resource plans (WLM) for long-running query triggers (kill queries running over N minutes or scanning over N TB), expose curated views/marts instead of raw tables, and educate users to preview with `LIMIT` and partition filters.

---

### Q196. Scenario: a Hive table's schema changed upstream (new column added). How do you roll it out safely?
**Answer:** Add the column at the end (`ALTER TABLE ... ADD COLUMNS`), handle historical partitions (returns NULL; backfill if needed), make sure downstream queries and views use explicit column lists (no `SELECT *`), validate ORC/Parquet column mapping for each format, run the pipeline in a lower environment first, and notify consumers. For breaking changes (type change, rename), create a versioned table or use a view layer to maintain compatibility.

---

### Q197. Scenario: how do you implement data quality checks in a Hive pipeline?
**Answer:** After each load, run SQL assertions: row-count vs source and vs yesterday (tolerance band), null rate on required columns, uniqueness of business keys, valid ranges/domains, referential integrity (anti-join to dimension), and freshness of the max timestamp. Store results in an audit table, fail or quarantine bad data (route rejected rows to a `_rejects` table with a reason), and send alerts. Tools like Great Expectations, Deequ, or custom HQL checks inside Airflow tasks are common.

---

### Q198. Scenario: plan a migration from an on-prem Hadoop/Hive cluster to the cloud (AWS/Azure/GCP).
**Answer:**
1. **Assess**: inventory data volume, tables, jobs, dependencies, and SLAs; classify hot/cold data.
2. **Storage**: move HDFS data to S3/ADLS/GCS (`distcp`, DataSync, Transfer Appliance); convert to Parquet/ORC where needed.
3. **Catalog**: migrate the Hive Metastore (or use Glue/Unity/Dataproc Metastore) and verify table locations.
4. **Compute**: run Hive/Spark on EMR/Dataproc/HDInsight/Databricks with ephemeral, right-sized clusters; refactor jobs for object-store semantics (committers, no rename reliance).
5. **Orchestration and security**: port Oozie to Airflow/managed workflows; map Ranger/Kerberos to IAM/Entra/Lake Formation policies.
6. **Validation**: run old and new pipelines in parallel, compare row counts and checksums, then cut over by domain and decommission gradually.
7. **Optimize**: table formats (Iceberg/Delta), partition and file-size tuning, cost monitoring.

---

### Q199. Scenario: the cluster feels slow overall (queries and jobs queueing). What do you check?
**Answer:**
- **YARN**: queue utilization, pending applications, a few huge jobs hogging resources, preemption settings.
- **HDFS**: NameNode RPC latency and heap/GC, DataNode disk or network saturation, under-replicated blocks causing heavy re-replication.
- **Metastore/HiveServer2**: backing database slowness, too many partitions, connection exhaustion.
- **Nodes**: dead/unhealthy NodeManagers, disk full, swapping.
- **Workload**: new jobs with skew, small-file explosions, or unbounded queries.

Resolve the immediate blocker (kill or throttle offenders), then tune queues, add capacity, and put alerts on the key indicators.

---

### Q200. "Tell me about a Hadoop/Hive project you worked on." How do you answer?
**Answer:** Use a structured story:
- **Context**: business problem, data sources, daily volume (e.g., 200 GB/day, 3 TB total), SLA.
- **Architecture**: ingestion (Sqoop/Kafka/Spark), HDFS layout (raw → curated → marts), Hive tables (external raw, partitioned ORC curated), orchestration (Airflow/Oozie), consumers (BI, data science).
- **Your role**: specific parts you built (partitioning design, SCD2 merge, data quality framework, tuning).
- **Challenges and results**: a real problem (skew, small files, schema drift, duplicate loads) and the measurable fix (job time from 3 hours to 40 minutes, storage down 70% with ORC + Snappy).
- **Operations**: monitoring, alerting, reruns, backfills, security.
- **Learnings**: what you'd change now (table formats like Iceberg, cloud object storage, more automated quality checks).

Be prepared for follow-ups on trade-offs: why ORC, why those partition columns, how you handled failures and reruns.

---


