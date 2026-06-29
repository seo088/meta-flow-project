-- 메타 플로깅 플랫폼 PostgreSQL + PostGIS 초기화
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── 사용자 ──────────────────────────────────────────────
CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username      VARCHAR(50)  UNIQUE NOT NULL,
    email         VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    display_name  VARCHAR(100),
    avatar_url    TEXT,
    role          VARCHAR(20) DEFAULT 'user',
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ── 지오펜싱 구역 ────────────────────────────────────────
CREATE TABLE geofence_zones (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    zone_key         VARCHAR(50) UNIQUE NOT NULL,
    name             VARCHAR(100) NOT NULL,
    geom             GEOMETRY(POLYGON, 4326) NOT NULL,
    phase            INTEGER DEFAULT 1,
    bonus_multiplier DECIMAL(4,2) DEFAULT 1.0,
    is_active        BOOLEAN DEFAULT TRUE,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_zones_geom ON geofence_zones USING GIST(geom);

-- ── 쓰레기 제보 ─────────────────────────────────────────
CREATE TABLE trash_reports (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    reporter_id   UUID REFERENCES users(id) ON DELETE SET NULL,
    location      GEOMETRY(POINT, 4326) NOT NULL,
    zone_id       UUID REFERENCES geofence_zones(id),
    trash_type    VARCHAR(50),
    severity      VARCHAR(20),
    status        VARCHAR(30) DEFAULT 'pending',
    image_urls    TEXT[],
    video_url     TEXT,
    source        VARCHAR(30),
    ai_confidence DECIMAL(5,4),
    ai_label      JSONB,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_reports_location ON trash_reports USING GIST(location);
CREATE INDEX idx_reports_zone_status ON trash_reports(zone_id, status) WHERE status != 'completed';
CREATE INDEX idx_reports_created ON trash_reports(created_at DESC);

-- ── 청소 인증 ────────────────────────────────────────────
CREATE TABLE cleanups (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    report_id      UUID REFERENCES trash_reports(id) ON DELETE CASCADE,
    cleaner_id     UUID REFERENCES users(id) ON DELETE SET NULL,
    cleaner_type   VARCHAR(20),
    before_image   TEXT,
    after_image    TEXT,
    verify_score   DECIMAL(5,4),
    verified       BOOLEAN DEFAULT FALSE,
    points_awarded INTEGER DEFAULT 0,
    completed_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ── 드론 이벤트 ─────────────────────────────────────────
CREATE TABLE drone_events (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    drone_id       VARCHAR(50) NOT NULL,
    event_type     VARCHAR(30),
    location       GEOMETRY(POINT, 4326),
    zone_id        UUID REFERENCES geofence_zones(id),
    video_url      TEXT,
    detected_count INTEGER DEFAULT 0,
    payload        JSONB,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_drone_location ON drone_events USING GIST(location);
CREATE INDEX idx_drone_created  ON drone_events(created_at DESC);

-- ── 포인트 원장 ──────────────────────────────────────────
CREATE TABLE point_ledger (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id    UUID REFERENCES users(id) ON DELETE CASCADE,
    delta      INTEGER NOT NULL,
    reason     VARCHAR(100) NOT NULL,
    ref_id     UUID,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, ref_id, reason)
);
CREATE INDEX idx_ledger_user    ON point_ledger(user_id);
CREATE INDEX idx_ledger_created ON point_ledger(created_at DESC);

-- 포인트 집계 MV
CREATE MATERIALIZED VIEW user_points AS
SELECT user_id, SUM(delta) AS total_points
FROM point_ledger GROUP BY user_id;
CREATE UNIQUE INDEX ON user_points(user_id);

CREATE OR REPLACE FUNCTION refresh_user_points() RETURNS void AS $$
BEGIN REFRESH MATERIALIZED VIEW CONCURRENTLY user_points; END;
$$ LANGUAGE plpgsql;

-- ── 배지 ────────────────────────────────────────────────
CREATE TABLE badge_definitions (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    badge_key   VARCHAR(100) UNIQUE NOT NULL,
    name        VARCHAR(200) NOT NULL,
    description TEXT,
    icon_url    TEXT,
    rarity      VARCHAR(20) DEFAULT 'common'
);

CREATE TABLE user_badges (
    user_id   UUID REFERENCES users(id) ON DELETE CASCADE,
    badge_id  UUID REFERENCES badge_definitions(id),
    earned_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, badge_id)
);

-- ── 퀘스트 ──────────────────────────────────────────────
CREATE TABLE quest_definitions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    quest_key       VARCHAR(100) UNIQUE NOT NULL,
    title           VARCHAR(200) NOT NULL,
    description     TEXT,
    quest_type      VARCHAR(30),
    zone_id         UUID REFERENCES geofence_zones(id),
    target_count    INTEGER NOT NULL,
    reward_xp       INTEGER NOT NULL,
    reward_badge_id UUID REFERENCES badge_definitions(id),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE user_quests (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id      UUID REFERENCES users(id) ON DELETE CASCADE,
    quest_id     UUID REFERENCES quest_definitions(id),
    progress     INTEGER DEFAULT 0,
    is_completed BOOLEAN DEFAULT FALSE,
    completed_at TIMESTAMPTZ,
    started_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, quest_id)
);

-- ── SNS 게시물 ───────────────────────────────────────────
CREATE TABLE posts (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id     UUID REFERENCES users(id) ON DELETE CASCADE,
    report_id   UUID REFERENCES trash_reports(id),
    content     TEXT,
    image_urls  TEXT[],
    hashtags    TEXT[],
    location    GEOMETRY(POINT, 4326),
    zone_id     UUID REFERENCES geofence_zones(id),
    post_type   VARCHAR(30) DEFAULT 'report',
    like_count  INTEGER DEFAULT 0,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_posts_user    ON posts(user_id);
CREATE INDEX idx_posts_created ON posts(created_at DESC);
CREATE INDEX idx_posts_hashtag ON posts USING GIN(hashtags);
CREATE INDEX idx_posts_zone    ON posts(zone_id, created_at DESC);

-- ── SNS 댓글 ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS post_comments (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    post_id       UUID REFERENCES posts(id) ON DELETE CASCADE,
    user_id       UUID REFERENCES users(id) ON DELETE CASCADE,
    content       TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_post_comments_post ON post_comments(post_id, created_at ASC);

-- ── DM (Direct Messages) ────────────────────────────────
CREATE TABLE direct_messages (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sender_id     UUID REFERENCES users(id) ON DELETE CASCADE,
    recipient_id  UUID REFERENCES users(id) ON DELETE CASCADE,
    content       TEXT NOT NULL,
    is_read       BOOLEAN DEFAULT FALSE,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_dm_recipient ON direct_messages(recipient_id, created_at DESC);
CREATE INDEX idx_dm_sender    ON direct_messages(sender_id, created_at DESC);

-- ── 공공데이터 배포 이력 ─────────────────────────────────
CREATE TABLE opendata_exports (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    export_type  VARCHAR(50),
    file_url     TEXT NOT NULL,
    record_count INTEGER,
    date_from    DATE,
    date_to      DATE,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);
