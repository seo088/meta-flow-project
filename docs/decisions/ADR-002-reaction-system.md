# ADR-002: 리액션 시스템 — 항상 빨간 하트

**상태**: 채택  
**일자**: 2026-04-13  

## 결정
리액션 아이콘은 항상 ❤️(빨간)을 표시한다. 사용자가 리액션한 경우에만 해당 이모지로 변경.

## 이유
- 🤍(회색 하트)로 표시하면 "좋아요가 없다"와 "내가 안 눌렀다"가 혼동됨
- Instagram 스타일: 아이콘은 항상 동일, 카운트로 구분
- reaction-summary(이모지 요약 행)가 버튼과 중복되어 2줄로 표시되는 버그 → 제거

## 적용
- `feedCard()`: `❤️ ${count}` 고정, `p.liked` 시 `REACTIONS[p.my_reaction]`으로 교체
- `reaction-summary` CSS/JS 완전 제거
- `doReact()` 후 기존 summary 잔여물 cleanup
