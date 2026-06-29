#!/usr/bin/env python3
"""
시나리오 시뮬레이션 실행기.
Usage:
  python scripts/run_scenario.py --all
  python scripts/run_scenario.py --campus
  python scripts/run_scenario.py --drone
  python scripts/run_scenario.py --boss
  python scripts/run_scenario.py --mobility
"""
import asyncio
import os
import sys
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

from core.dummy.scenarios import ScenarioSimulator
from core.logger import get_logger

logger = get_logger("run-scenario")


async def main():
    parser = argparse.ArgumentParser(description="메타 플로깅 시나리오 시뮬레이터")
    parser.add_argument("--all", action="store_true", help="모든 시나리오 실행")
    parser.add_argument("--campus", action="store_true", help="시나리오1: 캠퍼스 플로깅")
    parser.add_argument("--drone", action="store_true", help="시나리오2: 드론 순찰")
    parser.add_argument("--boss", action="store_true", help="시나리오3: 보스 레이드")
    parser.add_argument("--mobility", action="store_true", help="시나리오4: 모빌리티 수거")
    parser.add_argument("--api", default="http://localhost:8000", help="API base URL")
    args = parser.parse_args()

    sim = ScenarioSimulator(api_base=args.api)

    try:
        if args.all:
            await sim.run_all()
        elif args.campus:
            await sim.scenario_campus_plogging()
        elif args.drone:
            await sim.scenario_drone_patrol()
        elif args.boss:
            await sim.scenario_boss_raid()
        elif args.mobility:
            await sim.scenario_mobility_collection()
        else:
            logger.info("사용법: python run_scenario.py --all 또는 --campus/--drone/--boss/--mobility")
    finally:
        await sim.close()


if __name__ == "__main__":
    asyncio.run(main())
