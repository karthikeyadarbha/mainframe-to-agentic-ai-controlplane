# mainframe-to-agentic-ai-controlplane

A production-grade architectural blueprint demonstrating how to securely and deterministically bridge the generational gap between legacy core banking mainframes and next-generation autonomous AI systems. 

## The Ultimate Aim of the Exercise
The goal of this end-to-end implementation is to prove that **Enterprise Agentic AI** (e.g., autonomous AML investigators and GraphRAG systems) can be safely powered by 40-year-old mainframe ledger data without compromising operational SLAs or regulatory compliance. 

By implementing a **Governance-as-Code Control Plane**, this architecture decouples legacy extraction constraints from modern AI consumption. It dynamically ingests high-volume binary mainframe files, structurally models them into a historized Data Vault, enforces semantic business rules, and applies zero-trust cryptographic masking—all in-flight. The result is a 100% auditable, fault-tolerant data pipeline that delivers mathematically pure data products to AI agents.

---

## The Dataset: SOTA Curated AML Financial Flows
To validate this architecture against real-world tier-1 banking scenarios, this pipeline leverages synthetic data modeled after the **AMLworld benchmark** (*Realistic Synthetic Financial Transactions for Anti-Money Laundering Models*). 

Unlike legacy benchmarks (such as AMLSim) that only emulate partial fragments of financial crime, the dataset processed by this engine is curated to meet modern State-of-the-Art (SOTA) standards by simulating the **complete, end-to-end semantic lifecycle of illicit financial flows:**

1. **Placement:** Simulates the initial injection of illicit funds from 9 specific predicate crimes (e.g., smuggling, extortion) into the financial ecosystem.
2. **Layering:** Emulates complex subgraph interactions (e.g., fan-in, fan-out, bipartite flows) to obscure the origin of funds.
3. **Integration:** Models the final deployment of illicit funds back into the legitimate economy.

### Semantic Transitivity & Ground-Truth Tagging
Crucially, the dataset enforces **Semantic Transitivity**. When an entity transfers illicit funds through multiple hops and co-mingles them with legitimate money, the "laundering" tag remains perfectly traceable across the network. By preserving these perfect ground-truth labels through our Silver (Data Vault) and Gold (Data Product) tiers, we guarantee that downstream Agentic AI models receive the exact, uncorrupted network topologies required for accurate GraphRAG forensic retrieval.

---

## Conceptual Design: Governance-as-Code
The platform replaces manual compliance checklists with executable, bi-lateral **Data Contracts** defined via declarative YAML configurations. 
* **Producer Contract:** Enforces structural, byte-level constraints at the ingestion boundary (Bronze layer).
* **Consumer Contract:** Enforces semantic business rules, downstream AI model expectations, and cryptographic PII masking before emission (Gold layer).
* **Circuit Breaker Pattern:** Rather than allowing pipeline failures during legacy schema drift or malformed records, the engine safely isolates structural and semantic anomalies into localized **Dead Letter Queues (DLQs)**, guaranteeing a 100% application survival rate.

## Architectural Approach: Medallion Data Vault 2.0
The engine implements a strict **3-Tier Medallion Data Vault 2.0 Architecture** utilizing open-source transactional **Delta Lake** tables on object storage to enforce ACID compliance, time-travel auditing, and schema boundaries:
1. **Bronze Layer (Raw Ingestion):** Ingests raw binary data streams, decodes legacy character sets, and acts as an immutable historical audit trail.
2. **Silver Layer (Integration/Historized Vault):** Cleanses data and maps it into Data Vault 2.0 components:
   * `Hubs`: Pure business keys (hashed via SHA-256).
   * `Links`: Transactional relationships.
   * `Satellites`: Temporal descriptive context and numerical ledger metrics.
3. **Gold Layer (Consumption Data Products):** Fuses the Vault satellite metrics with explicit semantic validation checks, applies SHA-256 cryptographic PII masking, and materializes zero-trust data products optimized for downstream analytical systems and AI agents.

## Mainframe Mechanics & The Role of Apache Spark
The pipeline simulates a daily high-concurrency batch export from a Tier-1 core banking engine (e.g., Finacle or Amdocs). The data arrives as rigid **35-byte binary EBCDIC physical blocks** utilizing compressed **COMP-3 (Packed Decimal)** formatting. 

Because sequential EBCDIC blobs and binary COMP-3 structures cannot be directly queried by cloud analytical engines without massive computational degradation, **Apache Spark (PySpark)** functions as our Massively Parallel Processing (MPP) distributed translation framework:
* **Binary File Streaming:** Reads multi-gigabyte sequential dumps using Spark's `binaryFile` input source, slicing exact 35-byte blocks across cluster nodes to bypass host mainframe compute overhead.
* **Vectorized EBCDIC Decoding:** Applies parallel byte decoding (`F.decode(..., "cp500")`) over distributed partitions, transforming legacy arrays directly into readable ASCII/UTF-8 strings.
* **Bitwise COMP-3 Unpacking:** Implements vectorized custom bitwise expressions (`F.shiftRight`, `F.get_byte`, logical AND `& 0x0F`) to mathematically shift and extract packed decimal nibbles globally in memory, scaling precision by 100.0 to render analytical double-precision floats natively.

---

## Running the engine in `.venv`

1. Create and activate the virtual environment:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt ```

2. Run the engine with Java 11 explicitly set:

```bash
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    PYSPARK_PYTHON=$(pwd)/.venv/bin/python \
    .venv/bin/python src/engine.py ```