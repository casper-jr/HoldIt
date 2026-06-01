"""
export/ 폴더의 CSV 파일을 GCS 버킷에 업로드하는 스크립트.
Cloud Run Job 파이프라인 마지막 단계에서 실행됨.
기존 GitHub Actions의 git push 역할을 대체.
"""
import os
import glob
from google.cloud import storage

BUCKET_NAME = os.getenv("GCS_BUCKET", "holdit-exports")
EXPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "export")


def upload_exports():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    csv_files = glob.glob(os.path.join(EXPORT_DIR, "*.csv"))
    if not csv_files:
        print("업로드할 CSV 파일이 없습니다.")
        return

    for path in csv_files:
        blob_name = os.path.basename(path)
        bucket.blob(blob_name).upload_from_filename(path)
        print(f"업로드 완료: {blob_name} → gs://{BUCKET_NAME}/{blob_name}")

    print(f"\n총 {len(csv_files)}개 파일 업로드 완료.")


if __name__ == "__main__":
    upload_exports()
