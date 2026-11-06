import time
from minio import Minio

c = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin123", secure=False)

for _ in range(40):
    try:
        if not c.bucket_exists("lake"):
            c.make_bucket("lake")
        print("Bucket lake OK")
        break
    except Exception as e:
        print("MinIO pas prêt, nouvel essai...", e)
        time.sleep(3)