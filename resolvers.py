import os
import re
import json
import time
import urllib.parse
import urllib.request
import subprocess
try:
    import requests
except ImportError:
    requests = None
from typing import Dict, Any, Tuple, Optional
from torrent_parser import bdecode_torrent_name
from config import sanitize_filename

def resolve_torbox_magnet(magnet_url: str, api_key: str, task: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    """Submits magnet or .torrent file to TorBox and returns direct HTTPS CDN download URL."""
    api_headers = {
        'Authorization': f'Bearer {api_key.strip()}',
        'User-Agent': 'Mozilla/5.0'
    }
    create_url = "https://api.torbox.app/v1/api/torrents/createtorrent"
    if magnet_url.endswith(".torrent") or os.path.isfile(magnet_url):
        with open(magnet_url, 'rb') as tf:
            r = requests.post(
                create_url,
                headers={'Authorization': f'Bearer {api_key.strip()}'},
                files={'file': tf},
                data={'seed': '0', 'allow_zip': 'false'},
                timeout=25
            )
            res = r.json()
    else:
        form_data = urllib.parse.urlencode({'magnet': magnet_url, 'seed': 0, 'allow_zip': 'false'}).encode('utf-8')
        req = urllib.request.Request(create_url, data=form_data, headers=api_headers, method='POST')
        with urllib.request.urlopen(req, timeout=15) as resp:
            res = json.loads(resp.read().decode('utf-8'))

    if not res.get('success'):
        raise ValueError(f"TorBox error: {res.get('detail')}")

    torrent_id = res['data']['torrent_id']
    poll_url = f"https://api.torbox.app/v1/api/torrents/mylist?id={torrent_id}&bypass_cache=true"
    target_file = None
    target_file_id = None
    torrent_name = None

    for _ in range(60):
        if task and task.get("cancelled"):
            return None, None
        time.sleep(2.5)
        preq = urllib.request.Request(poll_url, headers=api_headers)
        with urllib.request.urlopen(preq, timeout=15) as presp:
            pdata = json.loads(presp.read().decode('utf-8'))

        item = pdata.get('data')
        if not item:
            continue
        if isinstance(item, list) and len(item) > 0:
            item = item[0]

        torrent_name = item.get('name')
        state = item.get('download_state', '').lower()
        finished = item.get('download_finished') or state in ['completed', 'cached']

        files = item.get('files', [])
        if finished and files:
            video_files = [f for f in files if any(f.get('name', '').lower().endswith(ext) for ext in ['.mp4', '.mkv', '.avi', '.webm', '.mov', '.ts'])]
            if not video_files:
                video_files = files
            video_files.sort(key=lambda x: x.get('size', 0), reverse=True)
            target_file = video_files[0]
            target_file_id = target_file['id']
            break

    if not target_file_id:
        raise TimeoutError("TorBox did not finish processing the torrent within the time limit.")

    dl_req_url = f"https://api.torbox.app/v1/api/torrents/requestdl?token={api_key.strip()}&torrent_id={torrent_id}&file_id={target_file_id}&redirect=false"
    with urllib.request.urlopen(urllib.request.Request(dl_req_url, headers=api_headers), timeout=15) as d_resp:
        dl_res = json.loads(d_resp.read().decode('utf-8'))

    if not dl_res.get('success'):
        raise ValueError(f"Failed to get direct download link from TorBox: {dl_res.get('detail')}")

    direct_url = dl_res['data']
    file_title = target_file.get('name', torrent_name or "Torrent_Video")
    return direct_url, file_title

def resolve_realdebrid_magnet(magnet_url: str, api_key: str, task: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    """Submits magnet or .torrent file to Real-Debrid and returns direct HTTPS CDN download URL."""
    api_headers = {
        'Authorization': f'Bearer {api_key.strip()}',
        'User-Agent': 'Mozilla/5.0'
    }
    if magnet_url.endswith(".torrent") or os.path.isfile(magnet_url):
        add_url = "https://api.real-debrid.com/rest/1.0/torrents/addTorrent"
        with open(magnet_url, 'rb') as tf:
            t_data = tf.read()
        req = urllib.request.Request(add_url, data=t_data, headers=api_headers, method='PUT')
        with urllib.request.urlopen(req, timeout=15) as resp:
            res = json.loads(resp.read().decode('utf-8'))
    else:
        add_url = "https://api.real-debrid.com/rest/1.0/torrents/addMagnet"
        form_data = urllib.parse.urlencode({'magnet': magnet_url}).encode('utf-8')
        req = urllib.request.Request(add_url, data=form_data, headers=api_headers, method='POST')
        with urllib.request.urlopen(req, timeout=15) as resp:
            res = json.loads(resp.read().decode('utf-8'))

    torrent_id = res['id']
    sel_url = f"https://api.real-debrid.com/rest/1.0/torrents/selectFiles/{torrent_id}"
    sel_data = urllib.parse.urlencode({'files': 'all'}).encode('utf-8')
    urllib.request.urlopen(urllib.request.Request(sel_url, data=sel_data, headers=api_headers, method='POST'), timeout=15)

    info_url = f"https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}"
    links = []
    file_name = None
    for _ in range(60):
        if task and task.get("cancelled"):
            return None, None
        time.sleep(2.0)
        with urllib.request.urlopen(urllib.request.Request(info_url, headers=api_headers), timeout=15) as i_resp:
            idata = json.loads(i_resp.read().decode('utf-8'))
        file_name = idata.get('filename')
        if idata.get('status') == 'downloaded' and idata.get('links'):
            links = idata['links']
            break

    if not links:
        raise TimeoutError("Real-Debrid did not complete downloading/caching the torrent within the time limit.")

    unrest_url = "https://api.real-debrid.com/rest/1.0/unrestrict/link"
    u_data = urllib.parse.urlencode({'link': links[0]}).encode('utf-8')
    with urllib.request.urlopen(urllib.request.Request(unrest_url, data=u_data, headers=api_headers, method='POST'), timeout=15) as u_resp:
        u_res = json.loads(u_resp.read().decode('utf-8'))

    direct_url = u_res['download']
    final_title = u_res.get('filename', file_name or "Torrent_Video")
    return direct_url, final_title

def resolve_video_source(raw_input: str, task: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Resolves input URL, iframe, or magnet link.
    Returns:
      {
        'url': final_download_url,
        'title': extracted_title_or_None,
        'headers': dict_of_http_headers,
        'type': 'videm_hls' | 'm3u8' | 'ytdlp' | 'direct' | 'local_torrent',
        'videm_meta': dict (for token refreshing)
      }
    """
    clean_input = raw_input.strip()
    std_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    # 1. Magnet Link or Local .torrent File Resolution
    is_torrent_file = clean_input.endswith(".torrent") or os.path.isfile(clean_input) or (task and task.get("is_torrent_file"))
    if clean_input.startswith("magnet:?") or clean_input.startswith("bc://bt/") or is_torrent_file:
        provider = (task.get("debrid_provider") or "local").lower() if task else "local"
        key = (task.get("debrid_key") or "").strip() if task else ""

        magnet_title = None
        if clean_input.startswith("magnet:?"):
            dn_match = re.search(r'[?&]dn=([^&]+)', clean_input)
            if dn_match:
                magnet_title = urllib.parse.unquote_plus(dn_match.group(1))
        elif is_torrent_file and os.path.isfile(clean_input):
            try:
                with open(clean_input, "rb") as tf:
                    magnet_title = bdecode_torrent_name(tf.read())
            except Exception:
                pass
            if not magnet_title:
                magnet_title = os.path.splitext(os.path.basename(clean_input))[0]

        if "real" in provider and key:
            direct_url, file_title = resolve_realdebrid_magnet(clean_input, key, task=task)
            return {
                'url': direct_url,
                'title': file_title or magnet_title,
                'headers': std_headers,
                'type': 'direct',
                'videm_meta': None
            }
        elif "torbox" in provider and key:
            direct_url, file_title = resolve_torbox_magnet(clean_input, key, task=task)
            return {
                'url': direct_url,
                'title': file_title or magnet_title,
                'headers': std_headers,
                'type': 'direct',
                'videm_meta': None
            }
        else:
            return {
                'url': clean_input,
                'title': magnet_title or (f"Torrent_{task['id']}" if task else "Torrent_Video"),
                'headers': std_headers,
                'type': 'local_torrent',
                'videm_meta': None
            }

    # 2. Extract src if user pasted an iframe or HTML block
    iframe_match = re.search(r'<iframe[^>]*\bsrc=([^\s>]+)', clean_input, re.I)
    if iframe_match:
        clean_input = iframe_match.group(1).strip('"\'')
    elif 'videm.xyz' in clean_input:
        videm_url_m = re.search(r'https?://(?:www\.)?videm\.xyz/(?:embed/[^\s"\'<>]+|player\.php\?[^\s"\'<>]+|[0-9a-fA-F-]{36}|tt\d+)', clean_input)
        if videm_url_m:
            clean_input = videm_url_m.group(0).strip()

    # 2b. Auto-convert IMDb IDs / URLs and TMDB URLs to Videm embeds
    imdb_m = re.match(r'^(?:https?://(?:www\.)?imdb\.com/title/)?(tt\d{7,8})/?(?:\?.*)?$', clean_input.strip(), re.I)
    if imdb_m:
        clean_input = f"https://videm.xyz/embed/movie/{imdb_m.group(1)}"

    tmdb_tv_m = re.match(r'^https?://(?:www\.)?themoviedb\.org/tv/(\d+)[^/]*/season/(\d+)/episode/(\d+)', clean_input.strip(), re.I)
    if tmdb_tv_m:
        clean_input = f"https://videm.xyz/embed/tv/{tmdb_tv_m.group(1)}/{tmdb_tv_m.group(2)}/{tmdb_tv_m.group(3)}"
    else:
        tmdb_mov_m = re.match(r'^https?://(?:www\.)?themoviedb\.org/movie/(\d+)', clean_input.strip(), re.I)
        if tmdb_mov_m:
            clean_input = f"https://videm.xyz/embed/movie/{tmdb_mov_m.group(1)}"

    # 2c. Auto-convert VidSrc / vsembed embeds via data.vidsrc.sh API
    vidsrc_tv_m = re.search(r'(?:vidsrc\.(?:mov|cc|sh|to)|vsembed\.ru)/embed/tv/(\d+)[/-](\d+)[/-](\d+)', clean_input, re.I)
    if vidsrc_tv_m:
        tmdb_id, s_num, e_num = vidsrc_tv_m.group(1), vidsrc_tv_m.group(2), vidsrc_tv_m.group(3)
        try:
            api_u = f"https://data.vidsrc.sh/api.php?type=tv&tmdb={tmdb_id}&season={s_num}&episode={e_num}"
            req = urllib.request.Request(api_u, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=5) as r:
                d = json.loads(r.read().decode('utf-8')).get('data', {})
                resolved_id = d.get('imdb_id') or tmdb_id
                if task and not task.get('has_custom_title') and d.get('file_name'):
                    task['title'] = sanitize_filename(os.path.basename(d['file_name']))
                clean_input = f"https://videm.xyz/embed/tv/{resolved_id}/{s_num}/{e_num}"
        except Exception:
            clean_input = f"https://videm.xyz/embed/tv/{tmdb_id}/{s_num}/{e_num}"

    vidsrc_mov_m = re.search(r'(?:vidsrc\.(?:mov|cc|sh|to)|vsembed\.ru)/embed/movie/(\d+)', clean_input, re.I)
    if vidsrc_mov_m:
        tmdb_id = vidsrc_mov_m.group(1)
        try:
            api_u = f"https://data.vidsrc.sh/api.php?type=movie&tmdb={tmdb_id}"
            req = urllib.request.Request(api_u, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=5) as r:
                d = json.loads(r.read().decode('utf-8')).get('data', {})
                resolved_id = d.get('imdb_id') or tmdb_id
                if task and not task.get('has_custom_title') and d.get('file_name'):
                    task['title'] = sanitize_filename(os.path.basename(d['file_name']))
                clean_input = f"https://videm.xyz/embed/movie/{resolved_id}"
        except Exception:
            clean_input = f"https://videm.xyz/embed/movie/{tmdb_id}"

    parsed = urllib.parse.urlparse(clean_input)
    domain = parsed.netloc.lower()

    # 3. Videm.xyz resolution
    if 'videm.xyz' in domain:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://videm.xyz/',
        }

        path = parsed.path.strip('/')
        if path.startswith('embed/'):
            embed_url = clean_input
        elif path.startswith('player.php'):
            embed_url = clean_input
        elif '/' not in path and len(path) > 0:
            embed_url = f"https://videm.xyz/embed/movie/{path}"
        else:
            embed_url = clean_input

        html = None
        last_fetch_err = None
        for _ in range(3):
            try:
                req = urllib.request.Request(embed_url, headers=headers)
                with urllib.request.urlopen(req, timeout=18) as resp:
                    html = resp.read().decode('utf-8', errors='ignore')
                if html and len(html) > 500:
                    break
            except Exception as e:
                last_fetch_err = e
                time.sleep(1.5)

        if not html or len(html) <= 500:
            try:
                curl_cmd = [
                    'curl', '-s', '-L',
                    '-H', f"User-Agent: {headers['User-Agent']}",
                    '-H', f"Referer: {headers['Referer']}",
                    '-H', "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    embed_url
                ]
                res = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=20)
                if res.returncode == 0 and len(res.stdout) > 500:
                    html = res.stdout
            except Exception:
                pass

        if not html or len(html) <= 500:
            raise ValueError(f"Failed to access Videm embed ({embed_url}): {last_fetch_err or 'Empty response'}. (Please check if the URL is accessible)")

        m = re.search(r'var\s+Q\s*=\s*(\{.*?\});', html)
        if not m:
            raise ValueError("Could not extract player state (Q) from Videm link. Please ensure link is an embed or video ID.")

        Q = json.loads(m.group(1))
        title = Q.get('title') or (Q.get('ssr', {}) or {}).get('title')

        initial_servers = []
        if isinstance(Q.get('ssr'), dict) and isinstance(Q['ssr'].get('servers'), list):
            initial_servers = list(Q['ssr']['servers'])

        qs = f"type={Q['type']}&id={urllib.parse.quote(str(Q['id']))}&s={Q['s']}&e={Q['e']}&t={urllib.parse.quote(Q['t'])}"
        api_sources_url = f"https://videm.xyz/api.php?a=sources&{qs}"

        api_headers = dict(headers)
        api_headers['Accept'] = 'application/json, text/javascript, */*; q=0.01'
        api_headers['X-Requested-With'] = 'XMLHttpRequest'
        api_headers['Referer'] = embed_url

        valid_stream_url = None
        valid_ref = None
        tried_server_keys = set()
        working_servers = []
        failed_server_details = []

        def test_server(srv):
            nonlocal valid_stream_url, valid_ref
            ref = srv.get('ref')
            name = srv.get('name', 'Unknown')
            srv_key = srv.get('k') or name
            if not ref or srv_key in tried_server_keys:
                return False
            tried_server_keys.add(srv_key)

            play_url = f"https://videm.xyz/api.php?a=play&ref={urllib.parse.quote(ref)}&t={urllib.parse.quote(Q['t'])}"
            try:
                pl_req = urllib.request.Request(play_url, headers=api_headers)
                with urllib.request.urlopen(pl_req, timeout=12) as pl_resp:
                    pdata = json.loads(pl_resp.read().decode('utf-8'))
                cand_path = pdata.get('url')
                if not cand_path:
                    failed_server_details.append(f"{name} (no stream url)")
                    return False

                cand_url = urllib.parse.urljoin('https://videm.xyz/', cand_path)

                t_req = urllib.request.Request(cand_url, headers=headers)
                with urllib.request.urlopen(t_req, timeout=10) as t_resp:
                    if t_resp.status == 200:
                        first_bytes = t_resp.read(512).decode('utf-8', errors='ignore')
                        if '#EXT' in first_bytes:
                            working_servers.append({'name': name, 'ref': ref, 'url': cand_url})
                            if not valid_stream_url:
                                valid_stream_url = cand_url
                                valid_ref = ref
                            return True
                        else:
                            failed_server_details.append(f"{name} (invalid m3u8)")
                    else:
                        failed_server_details.append(f"{name} (HTTP {t_resp.status})")
            except Exception as se:
                err_msg = str(se)
                if hasattr(se, 'read'):
                    try:
                        err_msg += f" {se.read().decode('utf-8')}"
                    except Exception:
                        pass
                failed_server_details.append(f"{name} ({err_msg})")
            return False

        for srv in initial_servers:
            if test_server(srv) and valid_stream_url:
                break

        if not valid_stream_url:
            max_source_polls = 6
            for p in range(max_source_polls):
                query_params = []
                if p == 0 or p % 2 == 1:
                    query_params.append("pf=1")
                if p == 0:
                    query_params.append("refresh=1")
                poll_url = api_sources_url + ("&" + "&".join(query_params) if query_params else "")
                try:
                    preq = urllib.request.Request(poll_url, headers=api_headers)
                    with urllib.request.urlopen(preq, timeout=12) as presp:
                        sdata = json.loads(presp.read().decode('utf-8'))
                except Exception:
                    time.sleep(1.2)
                    continue

                if not title and sdata.get('title'):
                    title = sdata.get('title')

                servers = sdata.get('servers') or []
                new_found = False
                for srv in servers:
                    k = srv.get('k') or srv.get('name')
                    if k not in tried_server_keys:
                        new_found = True
                        if test_server(srv) and valid_stream_url:
                            break

                if valid_stream_url:
                    break

                if not sdata.get('more') and not new_found:
                    break

                time.sleep(1.2)

        if not valid_stream_url:
            is_uuid = bool(re.match(r'^[0-9a-fA-F-]{36}$', str(Q.get('id', ''))))
            hint = " (The ID appears to be an internal UUID. Videm only indexes movies/series using IMDb IDs like tt1375666 or TMDB IDs like 27205.)" if is_uuid else ""
            n_tried = len(tried_server_keys)
            details = "; ".join(failed_server_details[:4])
            ep_info = f" (Season {Q.get('s')}, Episode {Q.get('e')})" if Q.get('s') and Q.get('e') else ""
            if all("502" in d or "unavailable" in d.lower() for d in failed_server_details):
                raise ValueError(f"Videm streaming servers for '{title or Q.get('id')}'{ep_info} are temporarily offline (502 Bad Gateway / Upstream Unavailable). Tested {n_tried} servers: {details}. The host has not cached this episode yet. TIP: Use the 'Live Torrents & Packs' dropdown in the Search panel above or paste a torrent/magnet link below to download immediately.")
            raise ValueError(f"Videm returned no active live streams for ID '{Q.get('id')}'. (Tested {n_tried} unique servers: {details}){hint}. Please retry in a few moments.")

        return {
            'url': valid_stream_url,
            'title': title or "Videm_Video",
            'headers': {'Referer': 'https://videm.xyz/', 'User-Agent': headers['User-Agent']},
            'type': 'videm_hls',
            'videm_meta': {
                'ref': valid_ref,
                't': Q['t'],
                'api_headers': api_headers,
                'headers': headers,
                'backup_servers': working_servers
            }
        }

    # 4. Direct M3U8
    if '.m3u8' in parsed.path.lower() or 'm3u8' in parsed.query.lower():
        return {
            'url': clean_input,
            'title': None,
            'headers': std_headers,
            'type': 'm3u8',
            'videm_meta': None
        }

    # 5. Known video streaming platforms (YouTube, Vimeo, etc.)
    streaming_hosts = ['youtube.com', 'youtu.be', 'vimeo.com', 'dailymotion.com', 'dood', 'streamtape', 'mixdrop', 'ok.ru']
    if any(sh in domain for sh in streaming_hosts):
        return {
            'url': clean_input,
            'title': None,
            'headers': std_headers,
            'type': 'ytdlp',
            'videm_meta': None
        }

    # 6. Direct file URL default
    return {
        'url': clean_input,
        'title': None,
        'headers': std_headers,
        'type': 'direct',
        'videm_meta': None
    }
