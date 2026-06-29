#!/bin/bash
# Hook: index.html JS 구문 자동 검증
# PostToolUse hook으로 Edit/Write 후 자동 실행

HTML_FILE="/home/spark/research/meta-flow/platform/apps/web/index.html"

if [ ! -f "$HTML_FILE" ]; then
  exit 0
fi

result=$(node -e "
const fs=require('fs');
const h=fs.readFileSync('$HTML_FILE','utf8');
const m=h.match(/<script>([\s\S]*)<\/script>/);
if(!m){console.log('NO_SCRIPT');process.exit(0)}
try{new Function(m[1]);console.log('OK')}catch(e){console.error('JS_ERROR:',e.message);process.exit(1)}
" 2>&1)

if [ $? -ne 0 ]; then
  echo "❌ JS 구문 오류 감지: $result"
  exit 1
fi

exit 0
