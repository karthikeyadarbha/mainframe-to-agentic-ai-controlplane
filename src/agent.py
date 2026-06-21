import os
import duckdb
import uuid
import datetime
import json
import pandas as pd
from typing import TypedDict, Literal, Dict, Any, List, Set, Tuple
from pydantic import BaseModel, Field, ValidationError

# LangGraph & LangChain Imports
from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage
from langchain_ollama import ChatOllama

# =====================================================================
# 1. FROZEN DATA CONTRACTS (Governance Layer)
# =====================================================================
class GovernanceMetadata(BaseModel):
    model_version: str
    graph_context_id: str
    source_snapshot_ts: datetime.datetime
    compliance_tag: str

class SovereignRiskConsumerContract(BaseModel):
    transaction_id: str
    risk_score: float = Field(..., ge=0.0, le=1.0)
    adjudication_label: Literal['APPROVE', 'REVIEW', 'BLOCK']
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    adjudication_rationale: str = Field(..., description="Human-readable XAI justification.")
    governance_metadata: GovernanceMetadata

# =====================================================================
# 2. LANGGRAPH STATE SCHEMA
# =====================================================================
class AdjudicationState(TypedDict):
    transaction_id: str
    # DuckDB Extracted Data
    calculated_risk: float
    historical_confidence: float
    # Computed Metrics
    structural_confidence: float  # S_c
    semantic_confidence: float    # M_c
    final_risk_score: float
    # Explainability Trace
    rationale: str
    # Final Pydantic Payload
    contract_payload: Dict[str, Any]

# =====================================================================
# 3. THE HYBRID COMPUTE ENGINE (Batch, XAI, and Publishing)
# =====================================================================
class SovereignRiskEngine:
    def __init__(self, gold_table_path: str = "data/gold"):
        self.gold_table_path = gold_table_path
        
        # 1. Initialize Analytical Engine (DuckDB) and bind to Delta Lake
        self.con = duckdb.connect(database=':memory:')
        self._bind_physical_data_layer()
        
        # 2. Initialize Semantic Engine (Local Air-Gapped Ollama)
        print("🤖 [SYSTEM] Booting Local AI Reasoning Engine (Ollama)...")
        self.llm = ChatOllama(
            model="llama3.1",  
            temperature=0.1,   
            format="json"      
        )
        # Cache semantic reasoning by adjudication label to optimize inference
        self.reasoning_cache: Dict[str, Dict[str, Any]] = {}
        
        # 3. Compile the LangGraph Workflow
        self.agent_executor = self._build_langgraph()

    # --- RESILIENCE & CHECKPOINTING ---

    def _load_checkpoint(self, checkpoint_path: str) -> Tuple[Set[str], List[Dict[str, Any]]]:
        if not os.path.exists(checkpoint_path):
            return set(), []
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            processed_ids = set(payload.get("processed_ids", []))
            results = payload.get("results", [])
            print(f"♻️ [SYSTEM] Loaded checkpoint: {len(processed_ids)} processed records.")
            return processed_ids, results
        except Exception as e:
            print(f"⚠️ [WARNING] Failed to load checkpoint. Starting fresh. Error: {e}")
            return set(), []

    def _save_checkpoint(self, checkpoint_path: str, processed_ids: Set[str], results: List[Dict[str, Any]], completed: int, total: int) -> None:
        payload = {
            "completed": completed,
            "total": total,
            "processed_ids": sorted(processed_ids),
            "results": results,
            "updated_at": datetime.datetime.now().isoformat(),
        }
        os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
        with open(checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)

    # --- DATA LAYER INTEGRATION ---

    def _bind_physical_data_layer(self):
        """Dynamically installs the Delta extension and mounts the physical Gold Delta table."""
        if not os.path.exists(self.gold_table_path):
            raise FileNotFoundError(f"Physical Gold Data not found at: {self.gold_table_path}")

        self.con.execute("INSTALL delta;")
        self.con.execute("LOAD delta;")
        
        self.con.execute(f"""
            CREATE OR REPLACE VIEW live_gold_transactions AS 
            SELECT * FROM delta_scan('{self.gold_table_path}');
        """)

    # --- LANGGRAPH NODES ---

    def node_fetch_structural_data(self, state: AdjudicationState) -> AdjudicationState:
        """NODE 1: Extracts structural network metrics via zero-copy Delta read."""
        tx_id = state["transaction_id"]
        
        try:
            result = self.con.execute("""
                SELECT account_balance, risk_category
                FROM live_gold_transactions 
                WHERE transaction_hash_id = ?
            """, [tx_id]).fetchone()
        except Exception as e:
            raise RuntimeError(f"Database schema mismatch or read error: {e}")
        
        if not result:
            raise ValueError(f"Transaction {tx_id} missing from live Gold Layer.")

        account_balance, risk_category = result
        calc_risk = min(max(float(account_balance) / 300000.0, 0.0), 1.0)
        hist_confidence = 0.95 if str(risk_category).upper() == "LOW" else 0.75
        
        print(f"🗄️ [Node 1: DuckDB] Fetched {tx_id} -> Risk: {calc_risk:.2f}")
        
        return {
            "calculated_risk": float(calc_risk), 
            "historical_confidence": float(hist_confidence), 
            "structural_confidence": float(hist_confidence)
        }

    def node_semantic_reasoning(self, state: AdjudicationState) -> AdjudicationState:
        """NODE 2: Generates Explainable AI (XAI) rationale using Local LLM."""
        risk = state["calculated_risk"]
        intended_action = "BLOCK" if risk > 0.8 else "REVIEW" if risk > 0.6 else "APPROVE"

        # Check Cache to optimize batch performance
        if intended_action in self.reasoning_cache:
            cached = self.reasoning_cache[intended_action]
            return {
                "semantic_confidence": float(cached["semantic_confidence"]),
                "rationale": str(cached["rationale"]),
            }
        
        print(f"🧠 [Node 2: LangGraph] Requesting XAI Trace for {intended_action}...")
        
        prompt = f"""You are a strict banking compliance AI auditor. 
Analyze a transaction with a calculated structural risk of {risk}.
Under corporate policy, this triggers a '{intended_action}' action.
1. Provide a semantic confidence score (0.0 to 1.0) representing certainty.
2. Provide a 1-2 sentence 'rationale' explaining why this risk level justifies the {intended_action} action.
You must respond ONLY with a valid JSON object using this exact schema:
{{
    "semantic_confidence": <float>,
    "rationale": "<string>"
}}"""
        
        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            parsed_response = json.loads(response.content)
            m_c = float(parsed_response.get("semantic_confidence", 0.90))
            rationale = str(parsed_response.get("rationale", "No reasoning provided."))
        except Exception as e:
            print(f"⚠️ [WARNING] Local LLM parsing failed. Error: {e}")
            m_c = 0.965 if risk < 0.5 else 0.85 
            rationale = f"Heuristic fallback applied for risk level {risk:.2f}."

        self.reasoning_cache[intended_action] = {
            "semantic_confidence": m_c,
            "rationale": rationale,
        }
            
        print(f"⚖️ [Node 2: LangGraph] Semantic Confidence: {m_c}")
        return {"semantic_confidence": m_c, "rationale": rationale}

    def node_contract_enforcement(self, state: AdjudicationState) -> AdjudicationState:
        """NODE 3: Enforces Governance-as-Code data product schema."""
        s_c = state["structural_confidence"]
        m_c = state["semantic_confidence"]
        final_risk = state["calculated_risk"]
        explanation = state["rationale"]
        
        final_confidence = round((0.60 * s_c) + (0.40 * m_c), 3)
        
        if final_risk > 0.8: label = "BLOCK"
        elif final_risk > 0.6: label = "REVIEW"
        else: label = "APPROVE"
        
        prov_metadata = {
            "model_version": "v3.1.0-sovereign-xai",
            "graph_context_id": f"graph-ctx-{uuid.uuid4().hex[:12]}",
            "source_snapshot_ts": datetime.datetime.now(),
            "compliance_tag": "AML_2026_SOVEREIGN"
        }
        
        payload = {
            "transaction_id": state["transaction_id"],
            "risk_score": final_risk,
            "adjudication_label": label,
            "confidence_score": final_confidence,
            "adjudication_rationale": explanation,
            "governance_metadata": prov_metadata
        }
        
        try:
            validated_product = SovereignRiskConsumerContract(**payload)
            print("🛡️ [Node 3: Pydantic] Contract Validated.")
            return {"contract_payload": validated_product.model_dump()}
        except ValidationError as ve:
            raise RuntimeError(f"Contract Violation in Agent Loop: {ve}")

    # --- LANGGRAPH ORCHESTRATION ---

    def _build_langgraph(self):
        workflow = StateGraph(AdjudicationState)
        workflow.add_node("fetch_data", self.node_fetch_structural_data)
        workflow.add_node("reason", self.node_semantic_reasoning)
        workflow.add_node("enforce", self.node_contract_enforcement)
        workflow.set_entry_point("fetch_data")
        workflow.add_edge("fetch_data", "reason")
        workflow.add_edge("reason", "enforce")
        workflow.add_edge("enforce", END)
        return workflow.compile()

    def process_transaction(self, tx_id: str) -> Dict[str, Any]:
        """Processes a single transaction through the graph."""
        initial_state = {"transaction_id": tx_id}
        final_state = self.agent_executor.invoke(initial_state)
        return final_state["contract_payload"]

    def process_batch(self) -> List[Dict[str, Any]]:
        """Orchestrates vector execution over the entire Delta dataset."""
        print("\n🔄 [SYSTEM] Initiating Batch Adjudication...")
        
        tx_records = self.con.execute(
            "SELECT DISTINCT transaction_hash_id FROM live_gold_transactions ORDER BY transaction_hash_id"
        ).fetchall()
        
        if not tx_records:
            print("⚠️ [WARNING] No transactions found in the live Gold Layer.")
            return []
            
        tx_ids = [record[0] for record in tx_records]
        
        checkpoint_path = "data/batch_checkpoint.json"
        checkpoint_every = 50
        
        processed_ids, batch_results = self._load_checkpoint(checkpoint_path)
        tx_ids = [tx_id for tx_id in tx_ids if tx_id not in processed_ids]
        total_tx = len(tx_ids)
        
        print(f"📊 [SYSTEM] Remaining transactions to adjudicate: {total_tx}.\n")
        
        for idx, tx_id in enumerate(tx_ids, 1):
            print(f"\n--- [BATCH {idx}/{total_tx}] Executing Adjudication for: {tx_id} ---")
            try:
                result = self.process_transaction(tx_id)
                batch_results.append(result)
                processed_ids.add(tx_id)
                if idx % checkpoint_every == 0:
                    self._save_checkpoint(checkpoint_path, processed_ids, batch_results, idx, total_tx)
            except Exception as e:
                print(f"❌ [ERROR] Pipeline failure for {tx_id}: {e}")

        self._save_checkpoint(checkpoint_path, processed_ids, batch_results, total_tx, total_tx)
        if total_tx > 0 and os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)
                
        return batch_results

    # --- PUBLISHING LAYER ---

    def publish_data_product(self, batch_results: List[Dict[str, Any]], target_dir: str = "data/published_product"):
        """Registers results to DuckDB and materializes back out to physical Parquet."""
        if not batch_results:
            print("⚠️ [SYSTEM] No results to publish.")
            return
            
        print("\n💾 [SYSTEM] Publishing Data Product to analytical storage...")
        df = pd.DataFrame(batch_results)
        self.con.register('memory_results_df', df)
        
        self.con.execute("""
            CREATE OR REPLACE TABLE adjudicated_transactions AS 
            SELECT * FROM memory_results_df
        """)
        
        os.makedirs(target_dir, exist_ok=True)
        physical_file = os.path.join(target_dir, "adjudication_results.parquet")
        self.con.execute(f"COPY adjudicated_transactions TO '{physical_file}' (FORMAT PARQUET)")
        
        print(f"✅ [DATA PRODUCT PUBLISHED] Tabular results materialized at: {physical_file}")

if __name__ == "__main__":
    # Point the engine to the physical input Delta directory
    input_delta_path = "data/gold"
    
    try:
        engine = SovereignRiskEngine(gold_table_path=input_delta_path)
        all_results = engine.process_batch()
        
        print("\n==================================================")
        print(f"✅ BATCH COMPLETE: Successfully processed {len(all_results)} transactions.")
        print("==================================================")
        
        # 1. Save raw JSON dump for debugging/archive
        json_output = "data/batch_results.json"
        with open(json_output, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, default=str)
        print(f"📁 Saved raw JSON payload to: {json_output}")
            
        # 2. Materialize SQL Data Product out to Parquet storage
        engine.publish_data_product(all_results)
        
    except Exception as e:
        print(f"\n❌ [CRITICAL ERROR] Engine execution failed: {e}")