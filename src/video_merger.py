# video_merger.py
import os
import mimetypes
import urllib.parse
import tempfile
from pathlib import Path
from typing import Optional, List, Dict
from uuid import uuid4
from subprocess import run

import requests
import boto3
from botocore.config import Config

# =======================
#  Config R2 por entorno
# =======================
R2_ACCOUNT_ID        = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID     = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME       = os.environ.get("R2_BUCKET_NAME", "latentsync")
R2_PREFIX_DEFAULT    = os.environ.get("R2_PREFIX", "chunks/")
R2_PUBLIC_BASE_URL   = os.environ.get("R2_PUBLIC_BASE_URL")

FFMPEG  = os.environ.get("FFMPEG_PATH", "ffmpeg")

if not (R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_PUBLIC_BASE_URL):
    raise RuntimeError("Faltan variables R2_* necesarias en el entorno")

# =======================
#  Cliente S3 (R2)
# =======================
s3 = boto3.client(
    "s3",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    region_name="auto",
    config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
)

# =======================
#  Helpers
# =======================
def _run(cmd: list[str]) -> str:
    p = run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError((p.stdout or "") + "\n" + (p.stderr or ""))
    return p.stdout

def join_public_url(base: str, key: str) -> str:
    clean = base.rstrip("/")
    safe_key = "/".join(urllib.parse.quote(part) for part in key.split("/"))
    return f"{clean}/{safe_key}"

def upload_file_to_r2(local_path: Path, key: str) -> str:
    ctype, _ = mimetypes.guess_type(str(local_path))
    ctype = ctype or "application/octet-stream"
    with open(local_path, "rb") as f:
        s3.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=f, ContentType=ctype)
    return join_public_url(R2_PUBLIC_BASE_URL, key)

def download_to_path(url: str, dst: Path, max_bytes: int = 8_000_000_000):
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ValueError("URL debe ser http(s)")
    with requests.get(url, stream=True, timeout=(10, 600)) as r:
        r.raise_for_status()
        total = 0
        with open(dst, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_bytes:
                    raise ValueError(f"Archivo supera el límite de {max_bytes} bytes")
                f.write(chunk)

def _write_concat_list_file(paths: list[Path], list_path: Path):
    with open(list_path, "w", encoding="utf-8") as f:
        for p in paths:
            f.write(f"file '{p.as_posix()}'\n")

# =======================
#  API de módulo
# =======================
def merge_videos(
    *,
    videos: List[str],             # URLs http(s) de MP4 en orden
    output_key_prefix: Optional[str] = None,
    reencode: bool = True,         # True: H.264 + AAC (robusto); False: -c copy
    crf: str = "20",
    preset: str = "veryfast",
    aac_bitrate: str = "192k"
) -> Dict:
    """
    Junta N MP4 en un MP4 y sube a R2.
    Por defecto re-codifica (H.264 + AAC) para máxima compatibilidad (igual que tu /join).
    Si reencode=False, intenta 'copy' (requiere codecs y params idénticos).

    :return: {"url": ..., "key": ..., "size_bytes": ..., "content_type": "video/mp4"}
    """
    if len(videos) < 2:
        raise ValueError("Proporciona al menos dos URLs en 'videos'.")

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)

        # 1) Descargar inputs
        local_inputs: list[Path] = []
        for i, url in enumerate(videos, start=1):
            p = td_path / f"in_{i:03d}.mp4"
            download_to_path(url, p)
            local_inputs.append(p)

        # 2) Archivo de lista para concat
        list_file = td_path / "inputs.txt"
        _write_concat_list_file(local_inputs, list_file)

        # 3) Salida
        out_name = f"joined_{uuid4().hex}.mp4"
        out_path = td_path / out_name

        # 4) Unir
        if reencode:
            cmd = [
                FFMPEG, "-hide_banner", "-y",
                "-f", "concat", "-safe", "0",
                "-i", str(list_file),
                "-c:v", "libx264", "-preset", preset, "-crf", crf,
                "-c:a", "aac", "-b:a", aac_bitrate,
                "-movflags", "+faststart",
                str(out_path),
            ]
        else:
            cmd = [
                FFMPEG, "-hide_banner", "-y",
                "-f", "concat", "-safe", "0",
                "-i", str(list_file),
                "-c", "copy",
                "-movflags", "+faststart",
                str(out_path),
            ]
        try:
            _run(cmd)
        except Exception as e:
            if not reencode:
                raise RuntimeError(f"ffmpeg no pudo unir en modo copy, prueba reencode=True: {e}")
            raise RuntimeError(f"ffmpeg no pudo unir los MP4: {e}")

        # 5) Subida a R2
        key_prefix = output_key_prefix.strip("/") + "/" if output_key_prefix else R2_PREFIX_DEFAULT
        key = f"{key_prefix}{out_name}"
        public_url = upload_file_to_r2(out_path, key)

        return {
            "url": public_url,
            "key": key,
            "size_bytes": out_path.stat().st_size,
            "content_type": "video/mp4",
        }

# --- uso directo (debug local) ---
if __name__ == "__main__":
    # Ejemplo rápido (ajusta URLs reales)
    result = merge_videos(
        videos=[
            "https://example.com/v1.mp4",
            "https://example.com/v2.mp4",
        ],
        output_key_prefix="joins/",
        reencode=True
    )
    print(result)
