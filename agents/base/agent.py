"""
Base classes for agentic system to ensure reusability and consistent patterns.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import asyncio
import logging

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """Abstract base class for all agents in the system."""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.logger = logging.getLogger(f"{__name__}.{name}")
        
    @abstractmethod
    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the agent's primary function."""
        pass
    
    @abstractmethod
    def get_capabilities(self) -> List[str]:
        """Return list of capabilities this agent supports."""
        pass


class BaseMCPAgent(BaseAgent):
    """Base class for agents that interact with MCP servers."""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.mcp_servers_dir = self.config.get('mcp_servers_dir', '/opt/airflow/mcp_servers')
        
    async def call_mcp_tool(self, server_script: str, tool_name: str, arguments: Dict[str, Any]) -> str:
        """Generic method to call MCP tools."""
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
        import sys
        import os
        
        # Resolve the server script path
        server_script_path = os.path.join(self.mcp_servers_dir, f"{server_script}_server.py")
        
        # Create parameters for the MCP server
        params = StdioServerParameters(
            command=sys.executable,
            args=[server_script_path]
        )
        
        try:
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    result = await session.call_tool(tool_name, arguments)
                    # Extract text content from response
                    content_text = "\n".join(
                        block.text for block in result.content if hasattr(block, "text")
                    )
                    return content_text
        except Exception as e:
            self.logger.error(f"Error calling MCP tool {tool_name}: {str(e)}")
            raise


class BaseOperatorAgent(BaseAgent):
    """Base class for operator agents that perform specific operations."""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        
    async def execute_with_retry(self, context: Dict[str, Any], 
                                max_retries: int = 3) -> Dict[str, Any]:
        """Execute with retry logic."""
        for attempt in range(max_retries):
            try:
                return await self.execute(context)
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        return {}
