#!/bin/bash
# Cloud Run Job 진입점 스크립트
# 기존 GitHub Actions weekly_export.yml의 Steps를 대체
set -e  # 에러 발생 시 즉시 중단

KR_LIMIT=${KR_LIMIT:-200}
US_LIMIT=${US_LIMIT:-1000}

echo "========================================="
echo "HoldIt 주간 파이프라인 시작"
echo "KR: ${KR_LIMIT}개 / US: ${US_LIMIT}개"
echo "========================================="

echo "[0단계] DB 테이블 초기화 (없으면 생성)"
python3 -c "from models import Base; from database import engine; Base.metadata.create_all(engine)"

echo "[1단계] 한국 주식 데이터 수집"
python3 main.py fetch kr $KR_LIMIT

echo "[1단계] 미국 주식 데이터 수집"
python3 main.py fetch us $US_LIMIT

echo "[2단계] 가공 지표 계산"
python3 main.py process

echo "[3단계] 점수 산정"
python3 main.py score

echo "[4단계] CSV 내보내기"
python3 main.py export

echo "[5단계] GCS 버킷 업로드"
python3 gcs_upload.py

echo "========================================="
echo "파이프라인 완료"
echo "========================================="
