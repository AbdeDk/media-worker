# handler.py
import os
from typing import Any, Dict

import runpod  # SDK RunPod Serverless

from audio_splitter import process_audio_split
from video_merger import merge_videos


def _payload_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    RunPod Serverless normalmente envía el input en event["input"].
    Aun así soportamos el caso de que venga plano en la raíz.
    """
    if isinstance(event, dict) and "input" in event and isinstance(event["input"], dict):
        return event["input"]
    return event if isinstance(event, dict) else {}


def _as_bool(v, default=False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _error(message: str, code: str = "BadRequest", extra: Dict[str, Any] | None = None):
    out = {"ok": False, "error": {"code": code, "message": message}}
    if extra:
        out["error"]["extra"] = extra
    return out


def rp_handler(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handler principal para RunPod Serverless.
    Espera un JSON con:
      - task: "split_audio" | "merge_videos"

    split_audio:
      - segments (int >=1)
      - audio_url (http/https)
      - codec ("mp3"|"aac"|"copy") [default "mp3"]
      - quality (str) [default "2"]
      - ext (str) [default "mp3"]
      - first_inverted (bool) [default false]
      - video_duration (float, opcional)  O  video_url (http/https)
      - r2_prefix (str, opcional)

    merge_videos:
      - videos (lista de URLs http/https, min 2)
      - output_key_prefix (str, opcional)
      - reencode (bool) [default true]
      - crf (str) [default "20"]
      - preset (str) [default "veryfast"]
      - aac_bitrate (str) [default "192k"]
    """
    data = _payload_from_event(event)
    task = (data.get("task") or "").strip()

    if not task:
        return _error("Falta 'task' ('split_audio' o 'merge_videos').")

    try:
        if task == "split_audio":
            # Validaciones mínimas
            if "segments" not in data:
                return _error("Falta 'segments' (int >= 1).")
            if "audio_url" not in data:
                return _error("Falta 'audio_url' (http/https).")

            result = process_audio_split(
                segments=int(data["segments"]),
                audio_url=str(data["audio_url"]),
                codec=str(data.get("codec", "mp3")),
                quality=str(data.get("quality", "2")),
                ext=str(data.get("ext", "mp3")),
                first_inverted=_as_bool(data.get("first_inverted"), False),
                video_duration=(float(data["video_duration"]) if data.get("video_duration") is not None else None),
                video_url=(str(data["video_url"]) if data.get("video_url") else None),
                r2_prefix=(str(data["r2_prefix"]).strip("/") + "/" if data.get("r2_prefix") else None),
            )
            return {"ok": True, "task": task, "result": result}

        elif task == "merge_videos":
            vids = data.get("videos")
            if not isinstance(vids, list) or len(vids) < 2:
                return _error("Proporciona al menos dos URLs en 'videos' (lista).")

            result = merge_videos(
                videos=[str(u) for u in vids],
                output_key_prefix=(str(data["output_key_prefix"]).strip("/") if data.get("output_key_prefix") else None),
                reencode=_as_bool(data.get("reencode"), True),
                crf=str(data.get("crf", "20")),
                preset=str(data.get("preset", "veryfast")),
                aac_bitrate=str(data.get("aac_bitrate", "192k")),
            )
            return {"ok": True, "task": task, "result": result}

        else:
            return _error("Valor de 'task' inválido. Usa 'split_audio' o 'merge_videos'.")

    except Exception as e:
        # Devolvemos error controlado con el mensaje completo (útil para logs de ffmpeg/ffprobe)
        return _error(f"{type(e).__name__}: {e}", code="InternalError")


# Registrar handler para Serverless
runpod.serverless.start({"handler": rp_handler})