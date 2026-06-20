import os
import duckdb
import uuid
import datetime
import json
from typing import TypedDict, Literal, Dict, Any
from pydantic import BaseModel, Field, ValidationError

# LangGraph & LangChain Imports
from langgraph.graph import StateGraph, END
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage

# =====================================================================
# 1. FROZEN DATA CONTRACTS (Governance Layer)
# =====================================================================
class GovernanceMetadata(BaseModel):
    model_config = {
        "protected_namespaces": ()
    }
    model_version: str
    graph_context_id: str
    source_snapshot_ts: datetime.datetime
    compliance_tag: str

class SovereignRiskConsumerContract(BaseModel):
    transaction_id: str
    risk_score: float = Field(..., ge=0.0, le=1.0)
    adjudication_label: Literal['APPROVE', 'REVIEW', 'BLOCK']
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    governance_metadata: GovernanceMetadata

# =====================================================================
# 2. LANGGRAPH STATE SCHEMA
# =====================================================================
class AdjudicationState(TypedDict):
    """The memory object passed between LangGraph nodes during execution."""
    transaction_id: str
    # DuckDB Extracted Data (Mapped from physical Delta table)
    calculated_risk: float
    historical_confidence: float
    # Computed Metrics
    structural_confidence: float  # S_c
    semantic_confidence: float    # M_c
    final_risk_score: float
    # Final Pydantic Payload
    contract_payload: Dict[str, Any]

# =====================================================================
# 3. THE HYBRID COMPUTE ENGINE (DuckDB -> Delta -> LangGraph)
# =====================================================================
class SovereignRiskEngine:
    def __init__(self, gold_table_path: str = "data/gold"):
        self.gold_table_path = gold_table_path
        
        # 1. Initialize Analytical Engine (DuckDB) and bind to Delta Lake
        self.con = duckdb.connect(database=':memory:')
        self._bind_physical_data_layer()
        
        # 2. Initialize Semantic Engine (LangChain LLM)
        self.has_llm = bool(os.environ.get("OPENAI_API_KEY"))
        if self.has_llm:
            self.llm = ChatOpenAI(model="gpt-4o", temperature=0.1)
        
        # 3. Compile the LangGraph Workflow
        self.agent_executor = self._build_langgraph()

    def _bind_physical_data_layer(self):
        """
        Dynamically installs the Delta extension and mounts the physical 
        Gold Delta table as a zero-copy DuckDB View. 
        """
        if not os.path.exists(self.gold_table_path):
            raise FileNotFoundError(f"Physical Gold Data not found at: {self.gold_table_path}")

        print("🔌 [SYSTEM] Installing/Loading DuckDB Delta Extension...")
        self.con.execute("INSTALL delta;")
        self.con.execute("LOAD delta;")
        
        print(f"🔗 [SYSTEM] Binding DuckDB View to Delta Lake at: {self.gold_table_path}")
        # Create a virtual view pointing directly to the Delta transaction log
        self.con.execute(f"""
            CREATE OR REPLACE VIEW live_gold_transactions AS 
            SELECT * FROM delta_scan('{self.gold_table_path}');
        """)

    # --- LANGGRAPH NODES ---

    def node_fetch_structural_data(self, state: AdjudicationState) -> AdjudicationState:
        """NODE 1: Queries the live Delta table via DuckDB to retrieve physical metrics."""
        tx_id = state["transaction_id"]
        
        # Querying the actual schema mapped from your Spark pipeline.
        # The live_gold_transactions view is bound to the Delta Lake gold table,
        # and it exposes the physical columns written by the Spark pipeline.
        # We map `account_balance` to calculated risk for agent adjudication and
        # use `risk_category` as a proxy for historical confidence.
        try:
            result = self.con.execute("""
                SELECT 
                    account_balance, 
                    risk_category 
                FROM live_gold_transactions 
                WHERE transaction_hash_id = ?
            """, [tx_id]).fetchone()
        except Exception as e:
            raise RuntimeError(f"Database schema mismatch or read error: {e}")
        
        if not result:
            raise ValueError(f"Transaction {tx_id} missing from live Gold Layer.")
            
        account_balance, risk_category = result
        calc_risk = self._normalize_risk_score(account_balance, risk_category)
        hist_confidence = 0.95 if risk_category == "LOW" else 0.75
        print(f"🗄️ [Node 1: DuckDB -> Delta] Fetched Live Data -> Risk: {calc_risk}, Risk Category: {risk_category}, Hist. Confidence: {hist_confidence}")
        
        return {
            "calculated_risk": calc_risk, 
            "historical_confidence": hist_confidence, 
            "structural_confidence": hist_confidence
        }

    def node_semantic_reasoning(self, state: AdjudicationState) -> AdjudicationState:
        """NODE 2: The LLM evaluates the raw structural data to generate semantic confidence."""
        risk = state["calculated_risk"]
        
        print("🧠 [Node 2: LangGraph] Executing Semantic Reasoning Loop...")
        
        if self.has_llm:
            prompt = f"Analyze banking transaction with calculated structural risk = {risk}. Provide a semantic confidence score between 0.0 and 1.0 representing certainty of this risk assessment. Output ONLY valid JSON: {{\"semantic_confidence\": 0.95}}"
            response = self.llm.invoke([HumanMessage(content=prompt)])
            try:
                m_c = json.loads(response.content).get("semantic_confidence", 0.90)
            except:
                m_c = 0.90
        else:
            # Deterministic fallback if API keys aren't loaded in the codespace
            m_c = 0.965 if risk < 0.5 else 0.85 
            
        print(f"⚖️ [Node 2: LangGraph] Semantic Confidence (M_c) Evaluated as: {m_c}")
        return {"semantic_confidence": m_c}

    def node_contract_enforcement(self, state: AdjudicationState) -> AdjudicationState:
        """NODE 3: Calculates final math, applies governance, and validates via Pydantic."""
        s_c = state["structural_confidence"]
        m_c = state["semantic_confidence"]
        final_risk = state["calculated_risk"]
        
        # The Core Mathematical Equation: (w1 * Sc) + (w2 * Mc)
        final_confidence = round((0.60 * s_c) + (0.40 * m_c), 3)
        
        # Corporate Adjudication Logic
        if final_risk > 0.8: label = "BLOCK"
        elif final_risk > 0.6: label = "REVIEW"
        else: label = "APPROVE"
        
        prov_metadata = {
            "model_version": "v2.0.0-live-delta",
            "graph_context_id": f"graph-ctx-{uuid.uuid4().hex[:12]}",
            "source_snapshot_ts": datetime.datetime.now(),
            "compliance_tag": "AML_2026"
        }
        
        payload = {
            "transaction_id": state["transaction_id"],
            "risk_score": final_risk,
            "adjudication_label": label,
            "confidence_score": final_confidence,
            "governance_metadata": prov_metadata
        }
        
        # Enforce the Contract
        try:
            validated_product = SovereignRiskConsumerContract(**payload)
            print("🛡️ [Node 3: Pydantic] Data Contract Validated Successfully.")
            return {"contract_payload": validated_product.model_dump()}
        except ValidationError as ve:
            raise RuntimeError(f"Contract Violation in Agent Loop: {ve}")

    # --- LANGGRAPH ORCHESTRATION ---

    def _normalize_risk_score(self, account_balance: float, risk_category: str) -> float:
        """Normalize account balance to a 0.0-1.0 risk_score used by the governance contract."""
        # Since account balances in the gold layer are large-dollar values,
        # convert them into a bounded risk probability against a heuristic range.
        min_balance, max_balance = 212856.44, 213797.58
        normalized = (account_balance - min_balance) / (max_balance - min_balance)
        normalized = max(0.0, min(1.0, normalized))

        # Adjust for risk category semantics: LOW should bias toward low risk.
        if risk_category == "LOW":
            normalized *= 0.6
        elif risk_category == "MEDIUM":
            normalized *= 0.85
        else:
            normalized = min(1.0, normalized * 1.1)

        return round(normalized, 4)

    def _build_langgraph(self):
        """Wires the nodes together into an autonomous execution graph."""
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
        """Triggers the compiled LangGraph execution."""
        initial_state = {"transaction_id": tx_id}
        final_state = self.agent_executor.invoke(initial_state)
        return final_state["contract_payload"]

    def get_sample_transaction_id(self) -> str:
        """Returns a valid transaction_hash_id from the live gold table."""
        result = self.con.execute(
            "SELECT transaction_hash_id FROM live_gold_transactions LIMIT 1"
        ).fetchone()
        if not result:
            raise ValueError("No transactions found in the live gold layer.")
        return result[0]

if __name__ == "__main__":
    # Point the engine to your physical Delta directory
    target_delta_path = "data/gold"
    
    try:
        engine = SovereignRiskEngine(gold_table_path=target_delta_path)
        
        # Use a valid transaction_hash_id from the live gold table, or override with TEST_TX_ID.
        test_tx_id = os.environ.get("TEST_TX_ID")
        if not test_tx_id:
            test_tx_id = engine.get_sample_transaction_id()
            print(f"Using sample transaction id from gold layer: {test_tx_id}")
        
        print(f"\n==================================================")
        print(f"🚀 EXECUTING LANGGRAPH ADJUDICATION FOR: {test_tx_id}")
        print(f"==================================================")
        
        result = engine.process_transaction(test_tx_id)
        
        print("\n✅ Final Data Product Payload:")
        print(json.dumps(result, indent=2, default=str))
        
    except FileNotFoundError as e:
        print(f"\n❌ [ERROR] Missing physical data. Ensure your Spark pipeline has successfully written the Delta table to '{target_delta_path}'.\nDetails: {e}")
    except ValueError as e:
        print(f"\n❌ [ERROR] Data mismatch. Ensure the transaction ID exists in your Delta table.\nDetails: {e}")