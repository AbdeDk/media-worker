# audio_splitter.py
import os
import mimetypes
import urllib.parse
import tempfile
from pathlib import Path
from typing import Optional, List, Dict
from uuid import uuid4
from subprocess import run, CalledProcessError

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
FFPROBE = os.environ.get("FFPROBE_PATH", "ffprobe")

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

def media_duration_seconds(path: str) -> float:
    out = _run([
        FFPROBE, "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=nw=1:nk=1",
        path
    ]).strip()
    return float(out)

def hhmmss_ms(t: float) -> str:
    ms = int(round((t - int(t)) * 1000))
    s = int(t) % 60
    m = (int(t) // 60) % 60
    h = int(t) // 3600
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"

def compute_equalized_cycle_cuts(audio_dur: float, segments: int, cycle: float) -> list[float]:
    if segments == 1:
        return [0.0, audio_dur]
    eps = 1e-3
    max_k = int((audio_dur - eps) // cycle)
    if max_k < (segments - 1):
        raise ValueError("No hay ciclos suficientes (reduce segments o usa otro cycle).")
    ideal_len = audio_dur / segments
    ks: list[int] = []
    k_prev = 0
    for i in range(1, segments):
        ideal_time = i * ideal_len
        ideal_k = int(round(ideal_time / cycle))
        min_allowed = k_prev + 1
        max_allowed = max_k - ((segments - 1) - i)
        if min_allowed > max_allowed:
            min_allowed = max_allowed
        k = max(min(ideal_k, max_allowed), min_allowed)
        ks.append(k); k_prev = k
    return [0.0] + [round(k * cycle, 3) for k in ks] + [round(audio_dur, 3)]

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

def download_to_path(url: str, dst: Path, max_bytes: int = 1_500_000_000):
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ValueError("URL debe ser http(s)")
    with requests.get(url, stream=True, timeout=(10, 300)) as r:
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

def export_segments(input_audio: str, cuts: list[float], outdir: Path,
                    codec: str, quality: str, ext: str) -> list[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    outputs: list[Path] = []
    for i in range(len(cuts) - 1):
        ss = cuts[i]
        to = cuts[i + 1]
        dur = max(0.0, to - ss)
        out_file = outdir / f"part_{i+1:03d}.{ext}"

        cmd = [FFMPEG, "-hide_banner", "-y",
               "-ss", hhmmss_ms(ss), "-t", hhmmss_ms(dur),
               "-i", input_audio, "-vn"]
        if codec == "mp3":
            cmd += ["-acodec", "libmp3lame", "-q:a", quality]
        elif codec == "aac":
            cmd += ["-acodec", "aac", "-b:a", quality]
        elif codec == "copy":
            cmd += ["-c", "copy"]
        else:
            raise ValueError("codec inválido")
        cmd += ["-map_metadata", "-1", str(out_file)]
        _run(cmd)
        outputs.append(out_file)
    return outputs

# =======================
#  API de módulo
# =======================
def process_audio_split(
    *,
    segments: int,
    audio_url: str,
    codec: str = "mp3",          # "mp3" | "aac" | "copy"
    quality: str = "2",          # mp3: -q:a (0-2); aac: "192k" etc.
    ext: str = "mp3",
    first_inverted: bool = False,
    video_duration: Optional[float] = None,
    video_url: Optional[str] = None,
    r2_prefix: Optional[str] = None
) -> List[Dict]:
    """
    Replica la lógica de /process:
    - Descarga audio (y opcionalmente video para obtener el 'cycle')
    - Calcula cortes equalizados por ciclo
    - Exporta segmentos (ffmpeg) con codec/quality/ext
    - Sube cada segmento a R2
    - Devuelve [{"audio": url_publica, "start_with_inverted": bool}, ...]

    :return: lista de resultados en orden
    """
    if segments < 1:
        raise ValueError("segments debe ser >= 1")
    if codec not in ("mp3", "aac", "copy"):
        raise ValueError("codec inválido (usa mp3|aac|copy)")

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)

        # --- AUDIO ---
        audio_path = td_path / "in_audio"
        download_to_path(audio_url, audio_path)
        try:
            audio_dur = media_duration_seconds(str(audio_path))
        except Exception as e:
            raise RuntimeError(f"ffprobe error (audio): {e}")

        # --- CYCLE (duración de video o directa) ---
        if video_duration is not None:
            cycle = float(video_duration)
        else:
            if not video_url:
                raise ValueError("Pasa 'video_url' o 'video_duration'")
            video_path = td_path / "in_video"
            download_to_path(video_url, video_path)
            try:
                cycle = media_duration_seconds(str(video_path))
            except Exception as e:
                raise RuntimeError(f"ffprobe error (video): {e}")

        if cycle <= 0:
            raise ValueError("La duración del ciclo debe ser > 0")

        # Cortes
        try:
            cuts = compute_equalized_cycle_cuts(audio_dur, segments, cycle)
        except Exception as e:
            raise RuntimeError(str(e))

        # Export
        try:
            local_files = export_segments(
                str(audio_path), cuts, td_path / "chunks",
                codec, quality, ext
            )
        except Exception as e:
            raise RuntimeError(f"ffmpeg error: {e}")

        # Subir a R2
        prefix = (r2_prefix.strip("/") + "/") if r2_prefix else R2_PREFIX_DEFAULT
        results: List[Dict] = []
        for idx, local_path in enumerate(local_files, start=1):
            key = f"{prefix}{local_path.name}"
            public_url = upload_file_to_r2(local_path, key)
            seg_start = cuts[idx - 1]
            clips_passed = int(seg_start // cycle)
            inverted = (clips_passed % 2 == 1)
            if first_inverted:
                inverted = not inverted
            results.append({"audio": public_url, "key": key, "start_with_inverted": inverted})
        return results

# --- uso directo (debug local) ---
if __name__ == "__main__":
    # Ejemplo rápido (ajusta URLs reales)
    out = process_audio_split(
        segments=3,
        audio_url="https://example.com/audio.mp3",
        video_url="https://example.com/video.mp4",
        codec="mp3",
        quality="2",
        ext="mp3",
        first_inverted=False
    )
    print(out)
