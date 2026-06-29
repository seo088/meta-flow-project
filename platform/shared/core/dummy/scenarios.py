"""
시나리오 시뮬레이터 — 더미 이벤트를 실시간으로 파이프라인에 주입.
실제 드론/모빌리티가 없어도 전체 플로우를 검증할 수 있음.
"""
import asyncio
import random
import httpx
from datetime import datetime
from .generators import (
    ZONES, random_point_in_zone, gen_user_id,
    TRASH_TYPES, SEVERITIES, HASHTAGS,
)
from ..logger import get_logger

logger = get_logger("scenario-sim")


class ScenarioSimulator:
    """군산시 가상 시나리오 시뮬레이터."""

    def __init__(self, api_base: str = "http://localhost:8000"):
        self.api = api_base
        self.client = httpx.AsyncClient(timeout=30.0)

    async def scenario_campus_plogging(self, n_users: int = 10, n_reports: int = 30):
        """
        시나리오 1: 학생 플로깅 데이
        군산대 캠퍼스에서 학생들이 동시 플로깅.
        """
        logger.info(f"=== 시나리오 1: 캠퍼스 플로깅 ({n_users}명, {n_reports}건) ===")

        for i in range(n_reports):
            lat, lon = random_point_in_zone("KU_CAMPUS")
            data = {
                "lat": lat,
                "lon": lon,
                "trash_type": random.choice(TRASH_TYPES[:3]),
                "severity": random.choice(SEVERITIES[:3]),
                "content": f"[시뮬] 캠퍼스 플로깅 #{i+1}",
                "hashtags": "#군산대,#플로깅,#시뮬레이션",
            }
            try:
                r = await self.client.post(f"{self.api}/api/v1/posts/report", data=data)
                logger.info(f"  제보 {i+1}/{n_reports}: {r.status_code}")
            except Exception as e:
                logger.warning(f"  제보 실패: {e}")
            await asyncio.sleep(random.uniform(0.5, 2.0))

    async def scenario_drone_patrol(self, zone_key: str = "EUNPA", n_drones: int = 2):
        """
        시나리오 2: 드론 순찰 + 모빌리티 수거
        은파유원지/새만금 드론 순찰.
        """
        logger.info(f"=== 시나리오 2: 드론 순찰 ({zone_key}, {n_drones}대) ===")

        for d in range(n_drones):
            drone_id = f"DRONE-{d+1:02d}"
            lat, lon = random_point_in_zone(zone_key)
            payload = {
                "drone_id": drone_id,
                "video_url": f"rtsp://sim/{drone_id}/{datetime.utcnow().isoformat()}.mp4",
                "lat": lat,
                "lon": lon,
                "zone_id": zone_key,
            }
            try:
                r = await self.client.post(
                    f"{self.api}/webhook/video-upload", json=payload
                )
                logger.info(f"  드론 {drone_id} 업로드: {r.status_code}")
            except Exception as e:
                logger.warning(f"  드론 업로드 실패: {e}")
            await asyncio.sleep(1.0)

    async def scenario_boss_raid(self, zone_key: str = "SAEMANGEUM"):
        """
        시나리오 3: 보스 레이드 이벤트
        새만금 대형 폐기물 발견 → 보스 퀘스트.
        """
        logger.info(f"=== 시나리오 3: 보스 레이드 ({zone_key}) ===")

        lat, lon = random_point_in_zone(zone_key)
        data = {
            "lat": lat,
            "lon": lon,
            "trash_type": "large",
            "severity": "boss",
            "content": f"[시뮬] 대형 폐기물 발견! 보스 레이드 시작!",
            "hashtags": "#보스레이드,#새만금,#시뮬레이션",
        }
        try:
            r = await self.client.post(f"{self.api}/api/v1/posts/report", data=data)
            logger.info(f"  보스 제보: {r.status_code}")
        except Exception as e:
            logger.warning(f"  보스 제보 실패: {e}")

        # 3인 팀 참여 시뮬레이션
        for i in range(3):
            await asyncio.sleep(2.0)
            logger.info(f"  팀원 {i+1}/3 참여")

        logger.info("  보스 레이드 완료!")

    async def scenario_mobility_collection(self, zone_key: str = "GEUMGANG"):
        """
        시나리오 4: 자율주행 모빌리티 수거
        금강하구둑 RL 최적 경로 → 순차 수거.
        """
        logger.info(f"=== 시나리오 4: 모빌리티 수거 ({zone_key}) ===")

        lat, lon = random_point_in_zone(zone_key)
        payload = {
            "zone_key": zone_key,
            "start_lat": lat,
            "start_lon": lon,
            "agent_type": "mobility",
        }
        try:
            r = await self.client.post(
                f"{self.api}/route/request", json=payload
            )
            logger.info(f"  경로 요청: {r.status_code}")
        except Exception as e:
            logger.warning(f"  경로 요청 실패: {e}")

        # 수거 시뮬레이션
        for wp in range(5):
            await asyncio.sleep(1.0)
            logger.info(f"  웨이포인트 {wp+1}/5 도달, 수거 완료")

    async def run_all(self):
        """모든 시나리오 순차 실행."""
        await self.scenario_campus_plogging(n_users=5, n_reports=10)
        await asyncio.sleep(2)
        await self.scenario_drone_patrol("EUNPA", 2)
        await asyncio.sleep(2)
        await self.scenario_boss_raid("SAEMANGEUM")
        await asyncio.sleep(2)
        await self.scenario_mobility_collection("GEUMGANG")
        logger.info("=== 전체 시나리오 완료 ===")

    async def close(self):
        await self.client.aclose()
