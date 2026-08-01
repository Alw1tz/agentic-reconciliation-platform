#!/bin/bash

echo "🚀 Starting Minimal Agentic Architecture Demo"
echo "================================================"

# Run the minimal demonstration
echo "🧪 Running minimal pipeline demo..."
python3 demo_minimal.py

echo ""
echo "✅ Demo completed successfully!"
echo ""
echo "This demonstrates:"
echo "• Reusable base agent classes"
echo "• Modular pipeline structure" 
echo "• MCP-style operation patterns"
echo "• Human approval workflow"
echo "• Scalable architecture for multiple use cases"

echo ""
echo "For a full demonstration with real infrastructure, run:"
echo "1. Fill in .env with your credentials"
echo "2. docker compose up airflow-init"
echo "3. docker compose up -d"
echo "4. Trigger the reconciliation_dag from Airflow UI"