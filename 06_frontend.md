# 메타 플로깅 — 프론트엔드 설계

> **참조**: `02_backend.md` §5 Kong 라우팅, `05_gamification.md` §5 WebSocket 스펙  
> **스택**: React Native (Expo) · Next.js 14 · Vite + React (관리자)  
> **모노레포**: pnpm workspaces

---

## 1. 프론트엔드 모노레포 구조

```
apps/
├── mobile/          # React Native (Expo SDK 50)
│   ├── src/
│   │   ├── api/     # React Query 훅
│   │   ├── store/   # Zustand 전역 상태
│   │   ├── screens/ # 화면
│   │   └── components/
│   └── package.json
│
├── web/             # Next.js 14 (App Router)
│   ├── src/app/
│   └── package.json
│
└── admin/           # Vite + React (관리자 대시보드)
    ├── src/pages/
    └── package.json

packages/
├── types/           # 공유 TypeScript 타입 (백엔드 types.py 1:1 대응)
├── api-client/      # axios 인스턴스 + 엔드포인트 함수
├── hooks/           # useWebSocket, useGeofence 등
└── ui/              # 공통 컴포넌트
```

---

## 2. 공유 타입 (`packages/types/src/index.ts`)

```typescript
// 백엔드 shared/core/types.py 와 1:1 대응 — 프론트에서 중복 정의 금지

export type TrashType    = "plastic"|"food"|"general"|"large"|"unknown";
export type Severity     = "low"|"mid"|"high"|"boss";
export type ReportStatus = "pending"|"ai_verified"|"assigned"|"completed";
export type DataSource   = "sns"|"drone"|"mobility"|"unity_sim";
export type ZoneKey      = "KU_CAMPUS"|"EUNPA"|"SAEMANGEUM"|"GEUMGANG";
export type Rarity       = "common"|"rare"|"epic"|"legendary";

export interface GeoPoint { lat: number; lon: number; }

export interface TrashReport {
  id: string; reporterId?: string; location: GeoPoint;
  zoneId?: string; zoneName?: string;
  trashType: TrashType; severity: Severity; status: ReportStatus;
  imageUrls: string[]; source: DataSource;
  aiConfidence?: number; createdAt: string;
}

export interface Post {
  id: string; userId: string; displayName: string; avatarUrl?: string;
  content: string; imageUrls: string[]; hashtags: string[];
  zoneName?: string; postType: string; likeCount: number; createdAt: string;
  report?: Pick<TrashReport,"trashType"|"severity"|"status">;
}

export interface Zone {
  id: string; zoneKey: ZoneKey; name: string;
  bonusMultiplier: number; phase: number;
  stats?: { pending: number; completed: number; bossCount: number; };
}

export interface UserProfile {
  userId: string; displayName: string; avatarUrl?: string;
  totalXp: number; level: number; levelTitle: string; xpToNext: number;
  globalRank?: number; badges: Badge[];
}

export interface Badge {
  badgeKey: string; name: string; iconUrl?: string;
  rarity: Rarity; earnedAt: string;
}

export interface Quest {
  questKey: string; title: string; description: string;
  questType: "daily"|"weekly"|"zone"|"raid"|"drone";
  targetCount: number; rewardXp: number;
  progress: number; isCompleted: boolean;
  zoneName?: string; badgeName?: string;
}

export type WSMessage =
  | { type:"points_earned"; userId:string; delta:number; newTotal:number; reason:string;
      levelUp?: { from:number; to:number; newTitle:string } }
  | { type:"badge_earned"; userId:string; badge:Badge }
  | { type:"quest_completed"; userId:string; questKey:string; title:string; xpReward:number }
  | { type:"ranking_updated"; top3:Array<{rank:number; displayName:string; xp:number}> }
  | { type:"boss_appeared"; zoneKey:ZoneKey; zoneName:string; location:GeoPoint;
      questKey:string; xpReward:number }
  | { type:"pin_added"; report: TrashReport };
```

---

## 3. API 클라이언트 (`packages/api-client/src/client.ts`)

```typescript
/**
 * 모든 앱이 이 인스턴스를 사용. 직접 axios import 금지.
 */
import axios, { AxiosInstance } from "axios";

let _client: AxiosInstance;

export function createApiClient(baseURL: string, getToken: () => string|null): AxiosInstance {
  _client = axios.create({ baseURL, timeout: 15000 });
  _client.interceptors.request.use(cfg => {
    const t = getToken();
    if (t) cfg.headers.Authorization = `Bearer ${t}`;
    return cfg;
  });
  _client.interceptors.response.use(
    res => res.data,
    err => Promise.reject({
      code:    err.response?.data?.code    || "UNKNOWN_ERROR",
      message: err.response?.data?.error  || "서버 오류가 발생했습니다",
      status:  err.response?.status,
    })
  );
  return _client;
}

const api = () => _client;

export const snsApi = {
  createReport: (fd: FormData)          => api().post("/api/v1/posts/report", fd),
  getFeed:      (zoneId?: string, pg=1) => api().get("/api/v1/feed", { params:{zone_id:zoneId, page:pg} }),
  toggleLike:   (id: string)            => api().post(`/api/v1/posts/${id}/like`),
  search:       (q: string)             => api().get("/api/v1/search", { params: { q } }),
};

export const gameApi = {
  getProfile:      ()              => api().get("/api/v1/game/profile/me"),
  getRanking:      (pg=1, sz=50)   => api().get("/api/v1/game/ranking/global", { params:{page:pg,size:sz} }),
  getZoneRanking:  (zk: string)    => api().get(`/api/v1/game/ranking/zone/${zk}`),
  getMyQuests:     ()              => api().get("/api/v1/game/quests/me"),
  acceptQuest:     (qk: string)    => api().post(`/api/v1/game/quests/${qk}/accept`),
  getPointHistory: (pg=1)          => api().get("/api/v1/game/points/history", { params:{page:pg} }),
};

export const gisApi = {
  getZoneStats:   ()                         => api().get("/api/v1/zones/stats"),
  getNearbyPins:  (lat:number, lon:number, r=500) => api().get("/api/v1/pins/nearby", { params:{lat,lon,radius:r} }),
  checkZoneEntry: (lat:number, lon:number)   => api().post("/api/v1/gis/check-zone-entry", {lat,lon}),
  exportGeoJSON:  (zk?: string)             => api().get("/api/v1/gis/export/geojson", { params:{zone_key:zk} }),
};

export const dataApi = {
  getZoneStats:  ()          => api().get("/api/v1/statistics/zones"),
  getTimeseries: (days=30)   => api().get("/api/v1/statistics/timeseries", { params:{days} }),
  getOpenDataList: ()        => api().get("/api/v1/opendata/list"),
};
```

---

## 4. WebSocket 훅 (`packages/hooks/src/useWebSocket.ts`)

```typescript
import { useEffect, useRef, useCallback } from "react";
import { WSMessage } from "@meta-plogging/types";

export function useWebSocket(
  url: string,
  onMessage: (msg: WSMessage) => void,
  enabled = true
) {
  const ws    = useRef<WebSocket|null>(null);
  const timer = useRef<NodeJS.Timeout>();
  const ping  = useRef<NodeJS.Timeout>();

  const connect = useCallback(() => {
    if (!enabled || typeof WebSocket === "undefined") return;
    const socket = new WebSocket(url);
    ws.current = socket;

    socket.onopen  = () => {
      ping.current = setInterval(() => socket.send("ping"), 30_000);
    };
    socket.onmessage = e => {
      if (e.data === "pong") return;
      try { onMessage(JSON.parse(e.data)); } catch {}
    };
    socket.onclose = () => {
      clearInterval(ping.current);
      timer.current = setTimeout(connect, 5_000);  // 5초 후 재연결
    };
    socket.onerror = () => socket.close();
  }, [url, onMessage, enabled]);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(timer.current);
      clearInterval(ping.current);
      ws.current?.close();
    };
  }, [connect]);
}
```

---

## 5. 모바일 앱 — 지도 화면 (`apps/mobile/src/screens/Map/MapScreen.tsx`)

```tsx
import React, { useEffect, useState, useCallback } from "react";
import { View, Text, TouchableOpacity, StyleSheet, Alert } from "react-native";
import MapView, { Marker, PROVIDER_GOOGLE } from "react-native-maps";
import * as Location from "expo-location";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { gisApi } from "@meta-plogging/api-client";
import { useWebSocket } from "@meta-plogging/hooks";
import { TrashReport, WSMessage } from "@meta-plogging/types";
import { useZoneStore } from "../../store/zoneStore";
import PinDetailSheet from "./PinDetailSheet";
import ReportFormSheet from "./ReportFormSheet";

const SEV_COLOR: Record<string,string> = {
  boss:"#7c3aed", high:"#ef4444", mid:"#f97316", low:"#22c55e"
};

export default function MapScreen() {
  const [pos, setPos]         = useState<{lat:number;lon:number}|null>(null);
  const [pin, setPin]         = useState<TrashReport|null>(null);
  const [report, setReport]   = useState(false);
  const { currentZone, setCurrentZone } = useZoneStore();
  const qc = useQueryClient();

  // 핀 목록 (30초 자동 갱신)
  const { data: pins = [] } = useQuery<TrashReport[]>({
    queryKey: ["pins", pos],
    queryFn:  () => pos
      ? gisApi.getNearbyPins(pos.lat, pos.lon, 1000).then(r => r.data)
      : Promise.resolve([]),
    enabled:        !!pos,
    refetchInterval: 30_000,
  });

  // 구역 현황
  const { data: zones = [] } = useQuery({
    queryKey: ["zone-stats"],
    queryFn:  () => gisApi.getZoneStats().then(r => r.data),
    refetchInterval: 60_000,
  });

  // GPS 추적 + 구역 진입 감지
  useEffect(() => {
    (async () => {
      const { status } = await Location.requestForegroundPermissionsAsync();
      if (status !== "granted") return;
      Location.watchPositionAsync(
        { accuracy: Location.Accuracy.High, distanceInterval: 10 },
        async loc => {
          const { latitude: lat, longitude: lon } = loc.coords;
          setPos({ lat, lon });
          const res = await gisApi.checkZoneEntry(lat, lon);
          const zk  = res.data?.data?.zone_key;
          if (res.data.in_zone && zk !== currentZone) {
            setCurrentZone(zk);
            Alert.alert("🌿 구역 진입!",
              `${res.data.data.name} 입장!\n보너스: ×${res.data.data.bonus_multiplier}`);
          }
        }
      );
    })();
  }, []);

  // WebSocket 실시간 업데이트
  useWebSocket(
    currentZone
      ? `${process.env.EXPO_PUBLIC_WS_URL}/ws/zone/${currentZone}`
      : `${process.env.EXPO_PUBLIC_WS_URL}/ws/global`,
    useCallback((msg: WSMessage) => {
      if (msg.type === "pin_added" || msg.type === "boss_appeared") {
        qc.invalidateQueries({ queryKey: ["pins"] });
        if (msg.type === "boss_appeared")
          Alert.alert("💀 보스 출현!", `${msg.zoneName}\n보상: ${msg.xpReward} XP`);
      }
    }, [currentZone, qc])
  );

  return (
    <View style={s.wrap}>
      <MapView style={s.map} provider={PROVIDER_GOOGLE} showsUserLocation
        initialRegion={{ latitude:35.970, longitude:126.690, latitudeDelta:.05, longitudeDelta:.05 }}>
        {pins.map(p => (
          <Marker key={p.id} coordinate={{latitude:p.location.lat,longitude:p.location.lon}}
            onPress={() => setPin(p)}>
            <View style={[s.pin,{backgroundColor:SEV_COLOR[p.severity]??"#6b7280"}]}>
              <Text style={s.pinTxt}>
                {p.severity==="boss"?"💀":p.status==="completed"?"✅":"🗑️"}
              </Text>
            </View>
          </Marker>
        ))}
      </MapView>

      {/* 구역 칩 */}
      <View style={s.chips}>
        {(zones as any[]).map((z: any) => (
          <View key={z.zone_key} style={s.chip}>
            <Text style={s.chipTxt}>{z.name}: {z.pending}건</Text>
          </View>
        ))}
      </View>

      <TouchableOpacity style={s.btn} onPress={() => setReport(true)}>
        <Text style={s.btnTxt}>🌿 플로깅 제보</Text>
      </TouchableOpacity>

      {pin    && <PinDetailSheet pin={pin} onClose={() => setPin(null)} />}
      {report && <ReportFormSheet userLocation={pos} onClose={() => setReport(false)}
                    onSuccess={() => { setReport(false); qc.invalidateQueries({queryKey:["pins"]}); }} />}
    </View>
  );
}

const s = StyleSheet.create({
  wrap:    { flex: 1 },
  map:     { flex: 1 },
  pin:     { width:30,height:30,borderRadius:15,alignItems:"center",
             justifyContent:"center",borderWidth:2,borderColor:"#fff" },
  pinTxt:  { fontSize: 13 },
  chips:   { position:"absolute",top:60,left:12,flexDirection:"row",flexWrap:"wrap",gap:6 },
  chip:    { backgroundColor:"rgba(0,0,0,0.65)",paddingHorizontal:10,paddingVertical:4,borderRadius:20 },
  chipTxt: { color:"#fff",fontSize:11,fontWeight:"600" },
  btn:     { position:"absolute",bottom:100,alignSelf:"center",backgroundColor:"#059669",
             paddingHorizontal:28,paddingVertical:14,borderRadius:28 },
  btnTxt:  { color:"#fff",fontWeight:"700",fontSize:15 },
});
```

---

## 6. 관리자 대시보드 (`apps/admin/src/pages/Dashboard.tsx`)

```tsx
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState, useCallback } from "react";
import { gisApi, gameApi, dataApi } from "@meta-plogging/api-client";
import { useWebSocket } from "@meta-plogging/hooks";
import { WSMessage } from "@meta-plogging/types";

const C = { bg:"#0f172a", card:"#1e293b", border:"#334155",
            blue:"#38bdf8", green:"#4ade80", orange:"#f97316",
            purple:"#a78bfa", gold:"#fbbf24", text:"#e2e8f0", muted:"#64748b" };

export default function Dashboard() {
  const [counts, setCounts] = useState({reports:0,completed:0,pending:0,drones:0});
  const qc = useQueryClient();

  const { data: zoneStats } = useQuery({
    queryKey: ["admin-zones"],
    queryFn:  () => gisApi.getZoneStats().then(r => r.data),
    refetchInterval: 15_000,
  });
  const { data: ranking } = useQuery({
    queryKey: ["admin-ranking"],
    queryFn:  () => gameApi.getRanking(1, 10).then(r => r.data),
    refetchInterval: 30_000,
  });
  const { data: opendata } = useQuery({
    queryKey: ["opendata-list"],
    queryFn:  () => dataApi.getOpenDataList().then(r => r.data),
    refetchInterval: 300_000,
  });

  useWebSocket(
    `${import.meta.env.VITE_WS_URL}/ws/global`,
    useCallback((msg: WSMessage) => {
      if (msg.type === "pin_added")
        setCounts(c => ({...c, reports:c.reports+1, pending:c.pending+1}));
      if (msg.type === "quest_completed")
        setCounts(c => ({...c, completed:c.completed+1, pending:Math.max(0,c.pending-1)}));
    }, [])
  );

  const statCards = [
    { label:"총 제보",  value:counts.reports,   color:C.blue },
    { label:"처리완료", value:counts.completed,  color:C.green },
    { label:"대기중",   value:counts.pending,    color:C.orange },
    { label:"활성드론", value:counts.drones,     color:C.purple },
  ];

  return (
    <div style={{padding:24,background:C.bg,minHeight:"100vh",color:C.text}}>
      <h1 style={{fontSize:22,fontWeight:800,marginBottom:24}}>
        🌿 메타 플로깅 관리자 — 군산시
      </h1>

      {/* 통계 카드 */}
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",
                   gap:12,marginBottom:28}}>
        {statCards.map(s => (
          <div key={s.label} style={{background:C.card,borderRadius:12,padding:"16px 12px",
                                     border:`1px solid ${C.border}`,textAlign:"center"}}>
            <div style={{fontSize:26,fontWeight:800,color:s.color}}>{s.value}</div>
            <div style={{fontSize:12,color:C.muted,marginTop:4}}>{s.label}</div>
          </div>
        ))}
      </div>

      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:20}}>

        {/* 구역별 현황 */}
        <div style={{background:C.card,borderRadius:14,padding:18,border:`1px solid ${C.border}`}}>
          <div style={{fontSize:14,fontWeight:700,marginBottom:14}}>구역별 현황</div>
          {(zoneStats ?? []).map((z:any) => {
            const total = z.pending + z.completed + 1;
            return (
              <div key={z.zone_key} style={{marginBottom:10}}>
                <div style={{display:"flex",justifyContent:"space-between",fontSize:12,marginBottom:3}}>
                  <span style={{fontWeight:600}}>{z.name}</span>
                  <span style={{color:C.muted}}>{z.pending}건 대기</span>
                </div>
                <div style={{background:"#0f172a",borderRadius:4,height:6}}>
                  <div style={{background:C.green,height:"100%",borderRadius:4,
                               width:`${Math.round(z.completed/total*100)}%`}}/>
                </div>
              </div>
            );
          })}
        </div>

        {/* TOP 10 랭킹 */}
        <div style={{background:C.card,borderRadius:14,padding:18,border:`1px solid ${C.border}`}}>
          <div style={{fontSize:14,fontWeight:700,marginBottom:14}}>실시간 TOP 10</div>
          {(ranking ?? []).map((u:any) => (
            <div key={u.user_id} style={{display:"flex",alignItems:"center",
                                         gap:8,marginBottom:8,fontSize:13}}>
              <span style={{minWidth:24}}>
                {u.rank<=3?["🥇","🥈","🥉"][u.rank-1]:`${u.rank}.`}
              </span>
              <span style={{flex:1}}>{u.display_name}</span>
              <span style={{fontWeight:700,color:C.gold}}>
                🌿 {u.total_xp.toLocaleString()}
              </span>
            </div>
          ))}
        </div>

        {/* 공공데이터 배포 현황 */}
        <div style={{gridColumn:"1/-1",background:C.card,borderRadius:14,
                     padding:18,border:`1px solid ${C.border}`}}>
          <div style={{fontSize:14,fontWeight:700,marginBottom:14}}>AI-Ready 공공데이터 배포 이력</div>
          <table style={{width:"100%",fontSize:12,borderCollapse:"collapse"}}>
            <thead>
              <tr style={{color:C.muted,borderBottom:`1px solid ${C.border}`}}>
                {["타입","파일 URL","레코드 수","생성일"].map(h => (
                  <th key={h} style={{padding:"6px 8px",textAlign:"left"}}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(opendata ?? []).slice(0,10).map((d:any,i:number) => (
                <tr key={i} style={{borderBottom:`1px solid ${C.border}`}}>
                  <td style={{padding:"6px 8px"}}>
                    <span style={{background:"#1e3a5f",color:C.blue,
                                  padding:"2px 7px",borderRadius:4,fontWeight:600}}>
                      {d.export_type}
                    </span>
                  </td>
                  <td style={{padding:"6px 8px",color:C.muted,fontFamily:"monospace"}}>
                    {d.file_url.split("/").slice(-1)[0]}
                  </td>
                  <td style={{padding:"6px 8px"}}>{d.record_count?.toLocaleString()}</td>
                  <td style={{padding:"6px 8px",color:C.muted}}>
                    {new Date(d.created_at).toLocaleDateString("ko-KR")}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
```

---

## 7. 환경변수

```bash
# apps/mobile/.env
EXPO_PUBLIC_API_URL=http://192.168.x.x:8000
EXPO_PUBLIC_WS_URL=ws://192.168.x.x:8200
EXPO_PUBLIC_GOOGLE_MAPS_KEY=YOUR_KEY

# apps/admin/.env
VITE_API_URL=http://localhost:8000
VITE_WS_URL=ws://localhost:8200

# apps/web/.env.local
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8200
```

---

## 8. 루트 `package.json` (pnpm workspace)

```json
{
  "private": true,
  "scripts": {
    "dev:mobile":  "cd apps/mobile && expo start",
    "dev:web":     "cd apps/web && next dev",
    "dev:admin":   "cd apps/admin && vite",
    "build:web":   "cd apps/web && next build",
    "build:admin": "cd apps/admin && vite build",
    "type-check":  "tsc --noEmit -p tsconfig.base.json",
    "lint":        "eslint 'apps/**/*.{ts,tsx}' 'packages/**/*.{ts,tsx}'"
  },
  "workspaces": ["apps/*", "packages/*"]
}
```
