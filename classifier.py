import os
import re
import json
from typing import Dict, Any, Optional

def _clean_title_string(s: str) -> str:
    """Removes release tags, brackets, hashes, and formats to title case."""
    # 1. Remove bracketed groups e.g. [YTS.BZ], [eztv], (1080p), etc.
    s = re.sub(r'\[[^\]]*\]|\([^\)]*(?:p|bit|rip|x264|x265|aac)[^\)]*\)', ' ', s, flags=re.I)
    
    # 2. Strip standard scene keywords and everything after them if they appear as metadata
    scene_words = [
        r'\b(?:480p|576p|720p|1080p|2160p|4k|uhd)\b',
        r'\b(?:10bit|8bit|hdr|dv|dolby|imax)\b',
        r'\b(?:web-?dl|web-?rip|bluray|bdrip|brrip|dvdrip|hdtv|cam|ts)\b',
        r'\b(?:x264|x265|h264|h265|hevc|av1|xvid)\b',
        r'\b(?:aac|ac3|dts|ddp5\.1|dd5\.1|truehd|atmos|mp3|flac)\b',
        r'\b(?:yts|rarbg|eztv|galaxyrg|tgx|vostfr|subita)\b'
    ]
    for pattern in scene_words:
        s = re.sub(pattern, ' ', s, flags=re.I)

    # 3. Replace dots, underscores, dashes with spaces
    s = re.sub(r'[._-]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s.title()

def heuristic_classify(raw_name: str) -> Dict[str, Any]:
    """
    Deterministic zero-key regex classifier that parses standard scene releases.
    Returns:
    {
        "type": "movie" | "tv_series" | "other",
        "show_name": str,
        "season": int,
        "episode": int,
        "year": Optional[int],
        "title": str,
        "formatted_name": str
    }
    """
    # Remove file extension if present
    base, _ = os.path.splitext(raw_name)
    cleaned_base = base.strip()

    # 1. Check for TV Series pattern: S01E04, s01e04, 1x04, Season 1 Episode 4
    tv_patterns = [
        r'(?i)^(.*?)[._\s]+S(\d{1,2})[._\s]*E(\d{1,2})',
        r'(?i)^(.*?)[._\s]+(\d{1,2})x(\d{1,2})',
        r'(?i)^(.*?)[._\s]+Season[._\s]*(\d{1,2})[._\s]*Episode[._\s]*(\d{1,2})'
    ]

    for pat in tv_patterns:
        m = re.search(pat, cleaned_base)
        if m:
            raw_show = m.group(1)
            season = int(m.group(2))
            episode = int(m.group(3))
            show_name = _clean_title_string(raw_show)
            formatted = f"{show_name} S{season:02d}E{episode:02d}"
            return {
                "type": "tv_series",
                "show_name": show_name,
                "season": season,
                "episode": episode,
                "year": None,
                "title": formatted,
                "formatted_name": formatted
            }

    # 2. Check for TV Series Season Pack: S01, Season 1
    m_season = re.search(r'(?i)^(.*?)[._\s]+(?:S|Season)[._\s]*(\d{1,2})\b', cleaned_base)
    if m_season:
        raw_show = m_season.group(1)
        season = int(m_season.group(2))
        show_name = _clean_title_string(raw_show)
        formatted = f"{show_name} Season {season:02d}"
        return {
            "type": "tv_series",
            "show_name": show_name,
            "season": season,
            "episode": 0,
            "year": None,
            "title": formatted,
            "formatted_name": formatted
        }

    # 3. Check for Movie pattern: Title.2025.720p...
    m_movie = re.search(r'(?i)^(.*?)[._\s]+((?:19|20)\d{2})\b', cleaned_base)
    if m_movie:
        raw_movie = m_movie.group(1)
        year = int(m_movie.group(2))
        movie_name = _clean_title_string(raw_movie)
        formatted = f"{movie_name} ({year})"
        return {
            "type": "movie",
            "show_name": "",
            "season": 0,
            "episode": 0,
            "year": year,
            "title": movie_name,
            "formatted_name": formatted
        }

    # 4. Fallback to Other
    cleaned_other = _clean_title_string(cleaned_base)
    if not cleaned_other:
        cleaned_other = "Untitled Video"

    return {
        "type": "other",
        "show_name": "",
        "season": 0,
        "episode": 0,
        "year": None,
        "title": cleaned_other,
        "formatted_name": cleaned_other
    }

def _get_gemini_api_key() -> Optional[str]:
    """Retrieves Gemini API key from environment or Google Colab userdata."""
    key = os.environ.get("GEMINI_API_KEY")
    if key:
        return key.strip()
    try:
        from google.colab import userdata
        key = userdata.get('GEMINI_API_KEY')
        if key:
            return key.strip()
    except Exception:
        pass
    return None

def classify_media(title_or_filename: str) -> Dict[str, Any]:
    """
    Primary Media Classifier.
    Attempts classification with Gemini Model (free tier) if key is present;
    otherwise falls back to deterministic heuristic regex engine.
    """
    fallback_result = heuristic_classify(title_or_filename)
    api_key = _get_gemini_api_key()

    if not api_key:
        return fallback_result

    # Try Gemini API call
    try:
        prompt = (
            f"You are a media file name classifier. Given the file name or release title below, determine if it is a movie, tv_series, or other.\n"
            f"Format rules:\n"
            f"- If movie: formatted_name MUST be 'Movie Title (Year)' (e.g. 'Avatar Fire And Ash (2025)').\n"
            f"- If tv_series: formatted_name MUST be 'Show Name S01E04' (two-digit season and episode).\n"
            f"- If other: clean descriptive title.\n"
            f"Respond with ONLY a JSON object having keys: type ('movie'|'tv_series'|'other'), show_name, season (int or 0), episode (int or 0), year (int or null), title, formatted_name.\n"
            f"Input: {title_or_filename}"
        )

        # 1. Try google-genai SDK
        try:
            from google import genai
            client = genai.Client(api_key=api_key)
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt
            )
            text = response.text
        except Exception:
            # 2. Try legacy google.generativeai
            import google.generativeai as genai_legacy
            genai_legacy.configure(api_key=api_key)
            model = genai_legacy.GenerativeModel("gemini-1.5-flash")
            response = model.generate_content(prompt)
            text = response.text

        # Extract JSON from response
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            parsed = json.loads(match.group(0))
            if "type" in parsed and "formatted_name" in parsed:
                return {
                    "type": parsed.get("type", fallback_result["type"]).lower(),
                    "show_name": parsed.get("show_name", fallback_result["show_name"]),
                    "season": int(parsed.get("season", fallback_result["season"]) or 0),
                    "episode": int(parsed.get("episode", fallback_result["episode"]) or 0),
                    "year": parsed.get("year", fallback_result["year"]),
                    "title": parsed.get("title", fallback_result["title"]),
                    "formatted_name": parsed.get("formatted_name", fallback_result["formatted_name"])
                }
    except Exception as e:
        print(f"[Classifier] Gemini API call skipped ({e}). Used heuristic.")

    return fallback_result
