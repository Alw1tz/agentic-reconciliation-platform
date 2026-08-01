"""
Minimal pipeline implementation showing core architecture patterns.
"""
from typing import Dict, Any
import asyncio
import json
import os

# Import our reusable minimal agent
from agents.minimal.core_agent import MinimalCoreAgent

class MinimalReconPipeline:
    """Minimal reconciliation pipeline demonstrating the architecture."""
    
    def __init__(self):
        self.scout_agent = MinimalCoreAgent("scout")
        self.remediate_agent = MinimalCoreAgent("remediate") 
        self.slack_agent = MinimalCoreAgent("slack_poster")
        
    async def run_pipeline(self, diff_report: Dict[str, Any]) -> Dict[str, Any]:
        """Run the minimal reconciliation pipeline."""
        
        # Extract anomalies from the diff report
        mismatches = diff_report.get("amounts_match", {}).get("detail", [])
        
        if not mismatches:
            return {"status": "no_anomalies", "message": "No amount mismatches found"}
            
        # Process the first mismatch (in real system, would process all)
        top_mismatch = mismatches[0]
        
        print(f"🔍 Processing anomaly: {top_mismatch}")
        
        # Scout phase - analyze the issue
        scout_context = {"operation": "scout", "anomaly_data": top_mismatch}
        scout_result = await self.scout_agent.execute(scout_context)
        print(f"   Scout result: {scout_result['result']}")
        
        # Remediate phase - generate fix
        remediate_context = {"operation": "remediate", "anomaly_data": top_mismatch}
        remediate_result = await self.remediate_agent.execute(remediate_context)
        print(f"   Generated SQL: {remediate_result['sql_fix']}")
        
        # Post to Slack for approval
        message_data = {
            "content": f"Reconciliation anomaly detected:\n"
                      f"Order ID: {top_mismatch.get('order_id')}\n"
                      f"Source: {top_mismatch.get('source_amount')}\n"
                      f"Target: {top_mismatch.get('target_amount')}\n\n"
                      f"*Proposed Fix:*\n```{remediate_result['sql_fix']}```\n"
                      f"*Regression Test:*\n```{remediate_result['test_query']}```"
        }
        
        slack_context = {"operation": "post_to_slack", "message_data": message_data}
        slack_result = await self.slack_agent.execute(slack_context)
        print(f"   Posted to Slack: {slack_result['message_id']}")
        
        return {
            "status": "completed",
            "anomaly": top_mismatch,
            "fix": remediate_result['sql_fix'],
            "test": remediate_result['test_query'],
            "slack_message_id": slack_result['message_id'],
            "approved": False  # Would be set by actual approval process
        }

# Simple example function for quick testing
async def run_demo():
    """Run a quick demo of the pipeline."""
    
    # Sample diff report (as would come from Spark reconciliation)
    sample_report = {
        "amounts_match": {
            "detail": [
                {
                    "order_id": 1003,
                    "source_amount": "412.10",
                    "target_amount": "41.21"
                }
            ]
        }
    }
    
    pipeline = MinimalReconPipeline()
    result = await pipeline.run_pipeline(sample_report)
    
    print("\n🎯 Demo completed successfully!")
    print(f"Status: {result['status']}")
    print(f"Fix generated: {result['fix'][:50]}...")
    
    return result

if __name__ == "__main__":
    asyncio.run(run_demo())
