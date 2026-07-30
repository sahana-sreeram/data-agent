#!/usr/bin/env bash
# Generates and verifies a candidate repair for loan_portfolio via a real MCP client call
# against the deployed mcp-data-ops server -- the fast, reliable path proven live: ~25s total
# with the scripted model (create_candidate_repair almost instant, verify_candidate_repair
# does a real local-mode Spark rerun + real pytest run). Requires the incident already
# injected (scripts/demo/inject-bug.sh) and USE_SCRIPTED_MODEL=true set on mcp-data-ops:
#   oc set env deployment/mcp-data-ops -n data-agent USE_SCRIPTED_MODEL=true
#
# Requires: `oc` logged into the target cluster, and the `mcp` Python package installed
# locally (`pip install mcp`).
set -euo pipefail

NAMESPACE="${DATA_AGENT_NAMESPACE:-data-agent}"
PIPELINE_NAME="${1:-loan_portfolio}"

oc port-forward "svc/mcp-data-ops" -n "$NAMESPACE" 18001:8000 >/tmp/pf-mcp-data-ops-demo.log 2>&1 &
PF_PID=$!
trap 'kill "$PF_PID" 2>/dev/null' EXIT
sleep 4

python3 -c "
import asyncio, json
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client

PIPELINE_NAME = '$PIPELINE_NAME'

async def main():
    async with streamable_http_client('http://localhost:18001/mcp') as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print(f'--- create_candidate_repair({PIPELINE_NAME!r}) ---')
            result = await session.call_tool('create_candidate_repair', {
                'pipeline_name': PIPELINE_NAME,
                'approve_categories': ['SOURCE_CONTRACT_CHANGE'],
            })
            data = json.loads(result.content[0].text)
            print(f\"repair_status: {data.get('repair_status')}\")
            if data.get('repair_status') != 'APPLIED':
                print(json.dumps(data.get('repair_plan', {}), indent=2))
                return

            repair_id = data['repair_id']
            print(f'--- verify_candidate_repair({repair_id!r}) ---')
            result2 = await session.call_tool('verify_candidate_repair', {'repair_id': repair_id})
            print(result2.content[0].text)

asyncio.run(main())
"
