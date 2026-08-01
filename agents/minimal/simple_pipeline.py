"""
A minimal but complete pipeline demonstrating the architecture.
"""
from typing import Dict, Any
import asyncio

# Import core agent
from agents.minimal.core_agent import MinimalCoreAgent

class SimplePipeline:
    """Minimal pipeline that demonstrates the architecture."""
    
    def __init__(self):
        self.scout_agent = MinimalCoreAgent("scout")
        self.remediate_agent = MinimalCoreAgent("remediate")
        self.slack_agent = MinimalCoreAgent("slack_poster")
        
    async def run_pipeline(self, anomaly_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run the complete pipeline with minimal steps."""
        
        print("=== Starting Simple Pipeline ===")
        
        # Step 1: Scout - Analyze the anomaly
        print("[1] Scout Agent analyzing anomaly...")
        scout_context = {"operation": "scout", "anomaly_data": anomaly_data}
        scout_result = await self.scout_agent.execute(scout_context)
        print(f"   Scout result: {scout_result['result']}")
        
        # Step 2: Remediate - Generate fix
        print("[2] Remediate Agent generating SQL fix...")
        remediate_context = {"operation": "remediate", "anomaly_data": anomaly_data}
        remediate_result = await self.remediate_agent.execute(remediate_context)
        print(f"   Generated fix: {remediate_result['sql_fix']}")
        
        # Step 3: Post to Slack - Send for human approval
        print("[3] Posting to Slack for human approval...")
        message_data = {
            "content": f"Anomaly detected: {anomaly_data.get('type', 'unknown')}\n"
                      f"Fix: {remediate_result['sql_fix']}\n"
                      f"Test: {remediate_result['test_query']}"
        }
        slack_context = {"operation": "post_to_slack", "message_data": message_data}
        slack_result = await self.slack_agent.execute(slack_context)
        print(f"   Posted to Slack with ID: {slack_result['message_id']}")
        
        return {
            "status": "completed",
            "anomaly": anomaly_data,
            "fix": remediate_result['sql_fix'],
            "test": remediate_result['test_query'],
            "slack_message_id": slack_result['message_id']
        }

# For direct execution
async def main():
    """Example usage."""
    pipeline = SimplePipeline()
    
    # Sample anomaly data (as would come from your reconciliation process)
    sample_anomaly = {
        "type": "amount_mismatch",
        "severity": "high",
        "id": 1003,
        "source_value": "412.10",
        "target_value": "41.21"
    }
    
    result = await pipeline.run_pipeline(sample_anomaly)
    print("\n=== Pipeline Complete ===")
    print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
