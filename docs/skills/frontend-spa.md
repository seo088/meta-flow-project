# Skill: 프론트엔드 SPA 수정 가이드

> `platform/apps/web/index.html` 단일 파일 SPA 수정 시 참조

## 파일 구조

```
index.html (단일 파일)
├── <style> ... </style>          # CSS (line ~1-350)
├── <body> ... </body>            # HTML 패널/모달 (line ~350-930)
└── <script> ... </script>        # JS 전체 로직 (line ~930-끝)
```

## 수정 절차

1. **수정 전**: 변경할 영역의 line 번호 확인 (`Grep`으로 검색)
2. **수정**: `Edit` 도구로 정확한 문자열 매칭하여 수정
3. **검증**: 반드시 JS 구문 검증 실행
   ```bash
   cd /home/spark/research/meta-flow/platform
   node -e "const fs=require('fs');const h=fs.readFileSync('apps/web/index.html','utf8');const m=h.match(/<script>([\s\S]*)<\/script>/);try{new Function(m[1]);console.log('OK')}catch(e){console.log('ERROR:',e.message)}"
   ```
4. **확인**: 게이트웨이 캐시 방지 헤더가 적용되어 있으므로 브라우저 Ctrl+Shift+R로 확인

## 주요 영역 맵

| 영역 | 시작 키워드 | 역할 |
|------|------------|------|
| CSS 변수 | `:root` | 테마 색상, 폰트 |
| 피드 카드 | `function feedCard` | SNS 피드 렌더링 |
| 홈 로드 | `async function loadHome` | 홈 대시보드 |
| 퀘스트 | `async function openQuest` | 퀘스트 상세 |
| 프로필 | `async function showProfile` | 프로필 카드 |
| 댓글 | `function renderCommentHtml` | 댓글 렌더링 |
| 리액션 | `async function doReact` | 좋아요/이모지 |
| DM | `async function sendDmReply` | 메시지 전송 |
| 인증 | `function loadSession` | 세션 관리 |
| 탭 전환 | `function showTab` | 네비게이션 |

## UI 컨벤션

- **더보기 메뉴**: `⋯` 버튼 → `position:fixed` 드롭다운 (overflow:hidden 회피)
- **피드 액션**: ❤️ 💬 🔗 만 직접 표시, 나머지는 ⋯ 메뉴 안
- **댓글**: Instagram 스타일 인라인 (이름+내용 한 줄)
- **하트**: 항상 ❤️(빨간), 리액션 시 해당 이모지로 변경
- **모달**: `compose-overlay` 클래스, `z-index` 계층 (일반:200, DM:300, 정책:5000)

## 모바일 반응형 필수 규칙

- **max-width에 항상 min() 사용**: `max-width:min(640px, 100%)` — 뷰포트보다 큰 고정값 방지
- **-webkit-overflow-scrolling:touch 사용 금지**: iOS에서 별도 스크롤 레이어 생성 → 부모 overflow:hidden 무시
- **img 전역 max-width:100%**: `img,video,canvas,svg{max-width:100%;height:auto}` 유지
- **갤러리 이미지**: `width:50%` + `flex-shrink:0` 대신 `flex:0 0 calc(50% - 1px)` 사용
- **그리드 고정 칼럼 금지**: `repeat(4,1fr)` 대신 `repeat(auto-fit,minmax(100px,1fr))` 사용
- **패널/컨테이너**: `overflow-x:hidden` 필수 (html, body, .panel, .nav 모두)
- **3중 방어**: html → body → .panel 모두 overflow-x:hidden 적용

## 주의사항

- `overflow:hidden`이 있는 `.feed-card` 내부에서 드롭다운 사용 시 `position:fixed` 필수
- 템플릿 리터럴 내 ternary에서 세미콜론 위치 주의 (`'val';` → `'val'`)
- `userAvatar()` 함수로 아바타 렌더링 통일
