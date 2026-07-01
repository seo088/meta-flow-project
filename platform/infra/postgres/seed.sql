-- 지오펜싱 구역 (실 좌표 기반 폴리곤)
INSERT INTO geofence_zones (zone_key, name, geom, phase, bonus_multiplier) VALUES
('KU_CAMPUS', '군산대학교',
 -- 실 좌표: 군산시 대학로 558(미룡동) 캠퍼스 ≈ 126.682,35.945
 ST_GeomFromText('POLYGON((126.6740 35.9385, 126.6880 35.9385, 126.6880 35.9520, 126.6740 35.9520, 126.6740 35.9385))', 4326),
 1, 1.0),
('EUNPA', '은파유원지',
 -- 은파호수공원 ≈ 126.692,35.95 (KU_CAMPUS 동편, 비겹침)
 ST_GeomFromText('POLYGON((126.6880 35.9392, 126.7072 35.9392, 126.7072 35.9612, 126.6880 35.9612, 126.6880 35.9392))', 4326),
 2, 1.5),
('SAEMANGEUM', '새만금',
 ST_GeomFromText('POLYGON((126.5100 35.7100, 126.6900 35.7100, 126.6900 35.8900, 126.5100 35.8900, 126.5100 35.7100))', 4326),
 2, 2.0),
('GEUMGANG', '금강하구둑',
 ST_GeomFromText('POLYGON((126.6920 35.9420, 126.7280 35.9420, 126.7280 35.9780, 126.6920 35.9780, 126.6920 35.9420))', 4326),
 3, 1.8)
ON CONFLICT (zone_key) DO NOTHING;

-- 배지 정의
INSERT INTO badge_definitions (badge_key, name, description, rarity) VALUES
('first_report',       '첫 발걸음',        '첫 번째 제보',              'common'),
('report_5',           '탐정 Lv.1',       '5건 제보',                  'common'),
('report_20',          '탐정 Lv.2',       '20건 제보',                 'rare'),
('cleanup_first',      '첫 청소',          '첫 번째 청소 완료',          'common'),
('cleanup_10',         '청소왕',           '10번 청소 완료',             'rare'),
('eunpa_guardian',     '은파 수호자',      '은파유원지 5건 처리',         'rare'),
('saemangeum_warrior', '새만금 워리어',    '새만금 구역 미션 완료',       'epic'),
('geumgang_ranger',    '금강 레인저',      '금강하구둑 미션 완료',        'epic'),
('boss_killer',        '보스 킬러',        '보스 레이드 성공',            'legendary'),
('drone_hero',         '드론 영웅',        '드론 영상 5건 업로드',        'rare'),
('ku_champion',        '군산대 챔피언',    '군산대 구역 1위',             'epic'),
('eco_legend',         '에코 레전드',      'Lv.12 달성',                'legendary'),
('weekly_streak_4',    '4주 연속',         '주간 퀘스트 4주 연속',        'epic')
ON CONFLICT (badge_key) DO NOTHING;

-- 주간 퀘스트
INSERT INTO quest_definitions (quest_key, title, description, quest_type, target_count, reward_xp) VALUES
('weekly_plogging_3', '주간 플로깅 챌린지',   '이번 주 3건 이상 플로깅 완료',   'weekly', 3, 150),
('weekly_report_5',   '탐정 미션',            '이번 주 5건 쓰레기 제보',        'weekly', 5, 100),
('weekly_drone_1',    '드론 협력',             '드론 영상 1건 업로드',           'weekly', 1, 80)
ON CONFLICT (quest_key) DO NOTHING;

-- 구역별 퀘스트
INSERT INTO quest_definitions (quest_key, title, description, quest_type, zone_id, target_count, reward_xp)
SELECT 'eunpa_guardian_5', '은파 수호자', '은파유원지에서 5건 이상 처리', 'zone', id, 5, 200
FROM geofence_zones WHERE zone_key = 'EUNPA'
ON CONFLICT (quest_key) DO NOTHING;

INSERT INTO quest_definitions (quest_key, title, description, quest_type, zone_id, target_count, reward_xp)
SELECT 'saemangeum_mission_3', '새만금 미션', '새만금 구역에서 3건 처리', 'zone', id, 3, 250
FROM geofence_zones WHERE zone_key = 'SAEMANGEUM'
ON CONFLICT (quest_key) DO NOTHING;

INSERT INTO quest_definitions (quest_key, title, description, quest_type, zone_id, target_count, reward_xp)
SELECT 'geumgang_mission_3', '금강 레인저', '금강하구둑에서 3건 처리', 'zone', id, 3, 220
FROM geofence_zones WHERE zone_key = 'GEUMGANG'
ON CONFLICT (quest_key) DO NOTHING;
