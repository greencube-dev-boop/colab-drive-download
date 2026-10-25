import re
import urllib.parse
import urllib.request
import json
from typing import List, Dict, Any, Tuple

def search_media_imdb(query: str, media_type: str = "All") -> Tuple[List[str], Dict[str, Any], str]:
    query = (query or "").strip()
    if not query:
        return [], {}, "Please enter a search title."

    slug = urllib.parse.quote(re.sub(r'[^a-zA-Z0-9]+', '_', query.lower()).strip('_'))
    if not slug:
        return [], {}, "Invalid search query."

    first_char = slug[0]
    url = f"https://v3.sg.media-imdb.com/suggestion/{first_char}/{slug}.json"

    choices = []
    cache = {}
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        with urllib.request.urlopen(req, timeout=6) as r:
            data = json.loads(r.read().decode('utf-8'))
            for item in data.get('d', []):
                item_id = item.get('id', '')
                if not item_id.startswith('tt'):
                    continue
                q_type = str(item.get('q', '')).lower()
                is_tv = 'tv' in q_type or 'series' in q_type
                m_type = 'tv' if is_tv else 'movie'

                if media_type == '🎬 Movies' and is_tv:
                    continue
                if media_type == '📺 TV Series' and not is_tv:
                    continue

                title = item.get('l', 'Unknown')
                year = item.get('y', '')
                stars = item.get('s', '')
                type_icon = '📺 TV Series' if is_tv else '🎬 Movie'

                label = f"{type_icon}: {title}" + (f" ({year})" if year else "")
                if stars:
                    label += f" — {stars}"
                label += f" [{item_id}]"

                info = {
                    'id': item_id,
                    'title': title,
                    'year': year,
                    'type': m_type,
                    'stars': stars,
                    'poster': item.get('i', {}).get('imageUrl') if isinstance(item.get('i'), dict) else None,
                    'display': label
                }
                choices.append(label)
                cache[label] = info
        msg = f"Found {len(choices)} result(s) for '{query}'." if choices else f"No results found for '{query}'."
    except Exception as e:
        msg = f"Search error: {e}"
    return choices, cache, msg

def get_tv_seasons_data(imdb_id: str) -> Tuple[str, Dict[int, List[Dict[str, Any]]]]:
    try:
        url = f"https://api.tvmaze.com/lookup/shows?imdb={imdb_id}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=6) as r:
            show = json.loads(r.read().decode('utf-8'))

        show_id = show['id']
        ep_url = f"https://api.tvmaze.com/shows/{show_id}/episodes"
        with urllib.request.urlopen(urllib.request.Request(ep_url, headers={'User-Agent': 'Mozilla/5.0'}), timeout=6) as er:
            episodes = json.loads(er.read().decode('utf-8'))

        seasons = {}
        for ep in episodes:
            s_num = ep.get('season')
            e_num = ep.get('number')
            e_name = ep.get('name') or f"Episode {e_num}"
            if s_num is None or e_num is None:
                continue
            if s_num not in seasons:
                seasons[s_num] = []

            seasons[s_num].append({
                'season': s_num,
                'episode': e_num,
                'name': e_name,
                'embed_url': f"https://videm.xyz/embed/tv/{imdb_id}/{s_num}/{e_num}",
                'label': f"EP {e_num:02d} - {e_name}"
            })
        return show.get('name') or "TV Series", seasons
    except Exception:
        return "TV Series", {}
