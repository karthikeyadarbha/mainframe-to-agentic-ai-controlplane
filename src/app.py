from fastapi import FastAPI, HTTPException, status
from pydantic import ValidationError
import datetime
import uuid

# Import the contract models you just built in agent.py
from agent import SovereignRiskConsumerContract, GovernanceMetadata

app = FastAPI(
    title="SovereignRisk-AI™ Data Product Interface",
    version="2.0.0",
    description="Zero-Trust, DAMA-compliant automated fraud adjudication endpoint."
)

# Mock database/state for the active architecture context
MODEL_VERSION = "v2.0.0"
ACTIVE_GRAPH_CONTEXT = f"graph-ctx-{uuid.uuid4().hex[:12]}"

@app.post(
    "/api/v2/adjudicate",
    response_model=SovereignRiskConsumerContract,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a transaction for real-time contract validation and risk scoring."
)
async def adjudicate_transaction(transaction_hash_id: str, calculated_risk: float, agent_confidence: float):
    """
    Purpose:
        Exposes the SovereignRisk-AI™ engine as an addressable Data Product API.
    Input:
        transaction_hash_id (str): The inbound PII-masked transaction hash.
        calculated_risk (float): Risk probability score from the engine.
        agent_confidence (float): The reasoning confidence window.
    Output:
        SovereignRiskConsumerContract: A fully verified, lineage-injected contract payload.
    """
    # Programmatic execution mapping logic (Block B: Lineage)
    adjudication = "REVIEW" if calculated_risk > 0.7 else "APPROVE"
    if calculated_risk > 0.9:
        adjudication = "BLOCK"

    prov_metadata = {
        "model_version": MODEL_VERSION,
        "graph_context_id": ACTIVE_GRAPH_CONTEXT,
        "source_snapshot_ts": datetime.datetime.now(),
        "compliance_tag": "AML_2026"
    }
    
    contract_payload = {
        "transaction_id": transaction_hash_id,
        "risk_score": calculated_risk,
        "adjudication_label": adjudication,
        "confidence_score": agent_confidence,
        "governance_metadata": prov_metadata
    }
    
    try:
        # Enforce the data contract boundaries instantly at the interface gate
        validated_product = SovereignRiskConsumerContract(**contract_payload)
        return validated_product
    except ValidationError as ve:
        # Catch and surface structural anomalies immediately as a bad request
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Data Product Contract Violation",
                "spec": "consumer_v2.yaml",
                "failures": ve.errors()
            }
        )

if __name__ == "__main__":
    import uvicorn
    # Spin up the local web worker to host the addressable interface
    uvicorn.run(app, host="127.0.0.1", port=8000)