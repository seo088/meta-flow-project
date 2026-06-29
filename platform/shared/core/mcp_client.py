"""
공유 MCP 클라이언트 헬퍼 — 에이전트에서 중복되는 _mcp() 호출 패턴을 단일 모듈로 추출.
모든 에이전트는 이 모듈만 import해서 MCP 도구를 호출한다.
"""
import os
import json
import logging

logger = logging.getLogger(__name__)

MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8510/sse")


async def call_tool(tool_name: str, arguments: dict, mcp_url: str = None) -> dict:
    """
    MCP 서버의 도구를 SSE로 호출하고 결과를 dict로 반환.
    AI_MODE=dummy이면 MCP 서버 없이 스텁 반환.

    Args:
        tool_name:  호출할 MCP 도구 이름 (e.g. "detect_trash")
        arguments:  도구 인자 dict
        mcp_url:    MCP 서버 SSE URL (기본: MCP_SERVER_URL)
    Returns:
        도구 실행 결과 dict
    """
    url = mcp_url or MCP_SERVER_URL

    # 더미 모드: MCP 서버 없이도 동작
    if os.environ.get("AI_MODE") == "dummy":
        logger.info(f"[STUB] MCP call → {tool_name}({list(arguments.keys())})")
        return _dummy_response(tool_name, arguments)

    try:
        from mcp.client.sse import sse_client
        from mcp import ClientSession

        async with sse_client(url) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, arguments)
                return json.loads(result.content[0].text)
    except Exception as e:
        logger.error(f"MCP call failed ({tool_name}): {e}")
        return _dummy_response(tool_name, arguments)


def _dummy_response(tool_name: str, arguments: dict) -> dict:
    """MCP 서버 미연결 시 더미 응답 — 개발/테스트용."""
    import random

    responses = {
        "detect_trash": {
            "trash_type": random.choice(["plastic", "food", "general"]),
            "severity": random.choice(["low", "mid", "high"]),
            "confidence": round(random.uniform(0.6, 0.99), 3),
        },
        "verify_cleanup": {
            "verified": True,
            "score": round(random.uniform(0.7, 0.98), 3),
            "points_awarded": 50,
        },
        "get_zone_pins": [],
        "optimize_route": {
            "waypoints": [],
            "total_distance_km": 0.0,
            "estimated_time_min": 0,
        },
        "award_points": {
            "new_total": 100,
            "delta": arguments.get("delta", 0),
            "duplicate": False,
        },
        "trigger_opendata_export": {
            "job_id": "stub-job",
            "status": "queued",
        },
        "get_zone_statistics": [],
    }
    return responses.get(tool_name, {"status": "stub", "tool": tool_name})
