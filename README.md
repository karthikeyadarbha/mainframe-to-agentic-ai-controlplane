# mainframe-to-agentic-ai-controlplane

Serves as a production-grade blueprint for modern, decentralized financial data architectures. It demonstrates how to successfully bridge the generational gap between legacy core banking systems and next-generation autonomous models.

## Conceptual Design
The platform is built upon the **Governance-as-Code** paradigm, replacing manual compliance checklists with executable, bi-lateral **Data Contracts** defined via declarative YAML configurations. 
* **Producer Contract:** Enforces structural, byte-level constraints at the ingestion boundary (Bronze layer).
* **Consumer Contract:** Enforces semantic business rules, downstream AI model expectations, and cryptographic PII masking before emission (Gold layer).
* **Circuit Breaker Pattern:** Rather than allowing pipeline failures during legacy schema drift or malformed records, the engine safely isolates structural and semantic anomalies into localized **Dead Letter Queues (DLQs)**, guaranteeing a 100% application survival rate.

## Architectural Approach
The engine implements a strict **3-Tier Medallion Data Vault 2.0 Architecture** utilizing open-source transactional **Delta Lake** tables on object storage to enforce ACID compliance, time-travel auditing, and schema boundaries:
1. **Bronze Layer (Raw Ingestion):** Ingests raw binary data streams, decodes legacy character sets, and acts as an immutable historical audit trail.
2. **Silver Layer (Integration/Historized Vault):** Cleanses data and maps it into Data Vault 2.0 components:
   * `Hubs`: Pure business keys (hashed via SHA-256).
   * `Links`: Transactional relationships.
   * `Satellites`: Temporal descriptive context and numerical ledger metrics.
3. **Gold Layer (Consumption Data Products):** Fuses the Vault satellite metrics with explicit semantic validation checks, applies SHA-256 cryptographic PII masking, and materializes zero-trust data products optimized for downstream analytical systems and AI agents.

## Detailed Source Information & Mainframe File Format
The pipeline simulates a daily high-concurrency batch export from a Tier-1 core banking engine like **Finacle**. 
* **File Format:** It processes massive, sequential binary flat files structured in rigid **35-byte physical record blocks**.
* **Character Encoding:** Text attributes (Transaction ID, Customer ID, Ledger Status) are encoded using IBM **EBCDIC (cp500)** code pages rather than ASCII/UTF-8.
* **Packed Decimals (COMP-3):** To maximize sequential mainframe throughput and storage, financial balances utilize COBOL COMP-3 formatting—compressing two numeric digits into a single hexadecimal byte, finalizing with an explicit sign nibble (e.g., `0xC` for positive, `0x0D` for negative).

## Role of Apache Spark in Transforming into ASCII
Because sequential EBCDIC blobs and binary COMP-3 structures cannot be directly queried by cloud analytical engines without massive computational degradation, **Apache Spark (PySpark)** functions as a Massively Parallel Processing (MPP) distributed translation framework:
* **Binary File Streaming:** Reads multi-gigabyte sequential dumps using Spark's `binaryFile` input source, slicing exact 35-byte binary blocks across cluster nodes without host mainframe API overhead.
* **Vectorized EBCDIC Decoding:** Applies parallel byte decoding (`F.decode(..., "cp500")`) over distributed partitions, transforming legacy EBCDIC arrays directly into readable ASCII/UTF-8 string formats.
* **Bitwise COMP-3 Unpacking:** Implements vectorized custom bitwise expressions (`F.shiftRight`, `F.get_byte`, logical AND `& 0x0F`) to mathematically shift and extract packed decimal nibbles globally in memory. It detects sign nibbles, calculates standard currency values, and scales precision by 100.0, rendering analytical double-precision floats natively before persisting historized records into Delta tables.

---

## Running the engine in `.venv`

1. Create and activate the virtual environment:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt