import asyncio
import logging
import re
import sys
import random
import os
import urllib.parse
from urllib.parse import urlparse, urljoin
import base64
import binascii
import json
import ssl
import aiohttp
from aiohttp import web, ClientSession, ClientTimeout, TCPConnector, ClientPayloadError, ServerDisconnectedError, ClientConnectionError
from aiohttp_proxy import ProxyConnector

from config import GLOBAL_PROXIES, TRANSPORT_ROUTES, get_proxy_for_url, get_ssl_setting_for_url, API_PASSWORD, check_password, MPD_MODE
from extractors.generic import GenericHLSExtractor, ExtractorError
from services.manifest_rewriter import ManifestRewriter

# Legacy MPD converter (used when MPD_MODE=legacy)
MPDToHLSConverter = None
decrypt_segment = None
if MPD_MODE == "legacy":
    try:
        from utils.mpd_converter import MPDToHLSConverter
        from utils.drm_decrypter import decrypt_segment
        logger = logging.getLogger(__name__)
        logger.info("✅ Moduli legacy MPD caricati (mpd_converter, drm_decrypter)")
    except ImportError as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"⚠️ MPD_MODE=legacy ma moduli non trovati: {e}")

# --- Moduli Esterni ---
VavooExtractor, DLHDExtractor, VixSrcExtractor, PlaylistBuilder, SportsonlineExtractor = None, None, None, None, None
MixdropExtractor, VoeExtractor, StreamtapeExtractor, OrionExtractor, FreeshotExtractor = None, None, None, None, None

logger = logging.getLogger(__name__)

# Importazione condizionale degli estrattori
try:
    from extractors.freeshot import FreeshotExtractor
    logger.info("✅ Modulo FreeshotExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo FreeshotExtractor non trovato.")

try:
    from extractors.vavoo import VavooExtractor
    logger.info("✅ Modulo VavooExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo VavooExtractor non trovato. Funzionalità Vavoo disabilitata.")

try:
    from extractors.dlhd import DLHDExtractor
    logger.info("✅ Modulo DLHDExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo DLHDExtractor non trovato. Funzionalità DLHD disabilitata.")

try:
    from routes.playlist_builder import PlaylistBuilder
    logger.info("✅ Modulo PlaylistBuilder caricato.")
except ImportError:
    logger.warning("⚠️ Modulo PlaylistBuilder non trovato. Funzionalità PlaylistBuilder disabilitata.")
    
try:
    from extractors.vixsrc import VixSrcExtractor
    logger.info("✅ Modulo VixSrcExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo VixSrcExtractor non trovato. Funzionalità VixSrc disabilitata.")

try:
    from extractors.sportsonline import SportsonlineExtractor
    logger.info("✅ Modulo SportsonlineExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo SportsonlineExtractor non trovato. Funzionalità Sportsonline disabilitata.")

try:
    from extractors.mixdrop import MixdropExtractor
    logger.info("✅ Modulo MixdropExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo MixdropExtractor non trovato.")

try:
    from extractors.voe import VoeExtractor
    logger.info("✅ Modulo VoeExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo VoeExtractor non trovato.")

try:
    from extractors.streamtape import StreamtapeExtractor
    logger.info("✅ Modulo StreamtapeExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo StreamtapeExtractor non trovato.")

try:
    from extractors.orion import OrionExtractor
    logger.info("✅ Modulo OrionExtractor caricato.")
except ImportError:
    logger.warning("⚠️ Modulo OrionExtractor non trovato.")

class HLSProxy:
    """Proxy HLS per gestire stream Vavoo, DLHD, HLS generici e playlist builder con supporto AES-128"""
    
    def __init__(self, ffmpeg_manager=None):
        self.extractors = {}
        self.ffmpeg_manager = ffmpeg_manager
        
        # Inizializza il playlist_builder se il modulo è disponibile
        if PlaylistBuilder:
            self.playlist_builder = PlaylistBuilder()
            logger.info("✅ PlaylistBuilder inizializzato")
        else:
            self.playlist_builder = None
        
        # Cache per segmenti di inizializzazione (URL -> content)
        self.init_cache = {}
        
        self.segment_cache = {}
        self.segment_cache_ttl = 60  # Seconds (increased from 30 for multi-key streams)
        
        # Prefetch queue for background downloading
        self.prefetch_tasks = set()
        
        # Sessione condivisa per il proxy
        self.session = None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            # Optimized connector with larger pool for concurrent segment downloads
            connector = TCPConnector(
                limit=100,  # Max total connections
                limit_per_host=30,  # Max per host
                keepalive_timeout=60,  # Keep connections alive longer
                enable_cleanup_closed=True
            )
            self.session = aiohttp.ClientSession(
                timeout=ClientTimeout(total=30),
                connector=connector
            )
        return self.session

    async def get_extractor(self, url: str, request_headers: dict, host: str = None):
        """Ottiene l'estrattore appropriato per l'URL"""
        try:
            # 1. Selezione Manuale tramite parametro 'host'
            if host:
                host = host.lower()
                key = host

                if host == "vavoo":
                    if key not in self.extractors:
                        self.extractors[key] = VavooExtractor(request_headers, proxies=GLOBAL_PROXIES)
                    return self.extractors[key]
                elif host in ["dlhd", "daddylive", "daddyhd"]:
                    key = "dlhd"
                    if key not in self.extractors:
                        self.extractors[key] = DLHDExtractor(request_headers, proxies=GLOBAL_PROXIES)
                    return self.extractors[key]
                elif host == "vixsrc":
                    if key not in self.extractors:
                        self.extractors[key] = VixSrcExtractor(request_headers, proxies=GLOBAL_PROXIES)
                    return self.extractors[key]
                elif host in ["sportsonline", "sportzonline"]:
                    key = "sportsonline"
                    if key not in self.extractors:
                        self.extractors[key] = SportsonlineExtractor(request_headers, proxies=GLOBAL_PROXIES)
                    return self.extractors[key]
                elif host == "mixdrop":
                    if key not in self.extractors:
                        self.extractors[key] = MixdropExtractor(request_headers, proxies=GLOBAL_PROXIES)
                    return self.extractors[key]
                elif host == "voe":
                    if key not in self.extractors:
                        self.extractors[key] = VoeExtractor(request_headers, proxies=GLOBAL_PROXIES)
                    return self.extractors[key]
                elif host == "streamtape":
                    if key not in self.extractors:
                        self.extractors[key] = StreamtapeExtractor(request_headers, proxies=GLOBAL_PROXIES)
                    return self.extractors[key]
                elif host == "orion":
                    if key not in self.extractors:
                        self.extractors[key] = OrionExtractor(request_headers, proxies=GLOBAL_PROXIES)
                    return self.extractors[key]
                elif host == "freeshot":
                    if key not in self.extractors:
                        self.extractors[key] = FreeshotExtractor(request_headers, proxies=GLOBAL_PROXIES)
                    return self.extractors[key]

            # 2. Auto-detection basata sull'URL
            if "vavoo.to" in url:
                key = "vavoo"
                proxy = get_proxy_for_url('vavoo.to', TRANSPORT_ROUTES, GLOBAL_PROXIES)
                proxy_list = [proxy] if proxy else []
                if key not in self.extractors:
                    self.extractors[key] = VavooExtractor(request_headers, proxies=proxy_list)
                return self.extractors[key]
            elif any(domain in url for domain in ["daddylive", "dlhd", "daddyhd"]) or re.search(r'watch\.php\?id=\d+', url):
                key = "dlhd"
                proxy = get_proxy_for_url('dlhd.dad', TRANSPORT_ROUTES, GLOBAL_PROXIES)
                proxy_list = [proxy] if proxy else []
                if key not in self.extractors:
                    self.extractors[key] = DLHDExtractor(request_headers, proxies=proxy_list)
                return self.extractors[key]
            elif 'vixsrc.to/' in url.lower() and any(x in url for x in ['/movie/', '/tv/', '/iframe/']):
                key = "vixsrc"
                proxy = get_proxy_for_url('vixsrc.to', TRANSPORT_ROUTES, GLOBAL_PROXIES)
                proxy_list = [proxy] if proxy else []
                if key not in self.extractors:
                    self.extractors[key] = VixSrcExtractor(request_headers, proxies=proxy_list)
                return self.extractors[key]
            elif any(domain in url for domain in ["sportzonline", "sportsonline"]):
                key = "sportsonline"
                proxy = get_proxy_for_url('sportsonline', TRANSPORT_ROUTES, GLOBAL_PROXIES)
                proxy_list = [proxy] if proxy else []
                if key not in self.extractors:
                    self.extractors[key] = SportsonlineExtractor(request_headers, proxies=proxy_list)
                return self.extractors[key]
            elif "mixdrop" in url:
                key = "mixdrop"
                proxy = get_proxy_for_url('mixdrop', TRANSPORT_ROUTES, GLOBAL_PROXIES)
                proxy_list = [proxy] if proxy else []
                if key not in self.extractors:
                    self.extractors[key] = MixdropExtractor(request_headers, proxies=proxy_list)
                return self.extractors[key]
            elif any(d in url for d in ["voe.sx", "voe.to", "voe.st", "voe.eu", "voe.la", "voe-network.net"]):
                key = "voe"
                proxy = get_proxy_for_url('voe.sx', TRANSPORT_ROUTES, GLOBAL_PROXIES)
                proxy_list = [proxy] if proxy else []
                if key not in self.extractors:
                    self.extractors[key] = VoeExtractor(request_headers, proxies=proxy_list)
                return self.extractors[key]
            elif "popcdn.day" in url:
                key = "freeshot"
                proxy = get_proxy_for_url('popcdn.day', TRANSPORT_ROUTES, GLOBAL_PROXIES)
                proxy_list = [proxy] if proxy else []
                if key not in self.extractors:
                    self.extractors[key] = FreeshotExtractor(request_headers, proxies=proxy_list)
                return self.extractors[key]
            elif "streamtape.com" in url or "streamtape.to" in url or "streamtape.net" in url:
                key = "streamtape"
                proxy = get_proxy_for_url('streamtape', TRANSPORT_ROUTES, GLOBAL_PROXIES)
                proxy_list = [proxy] if proxy else []
                if key not in self.extractors:
                    self.extractors[key] = StreamtapeExtractor(request_headers, proxies=proxy_list)
                return self.extractors[key]
            elif "orionoid.com" in url:
                key = "orion"
                proxy = get_proxy_for_url('orionoid.com', TRANSPORT_ROUTES, GLOBAL_PROXIES)
                proxy_list = [proxy] if proxy else []
                if key not in self.extractors:
                    self.extractors[key] = OrionExtractor(request_headers, proxies=proxy_list)
                return self.extractors[key]
            else:
                # ✅ MODIFICATO: Fallback al GenericHLSExtractor per qualsiasi altro URL.
                # Questo permette di gestire estensioni sconosciute o URL senza estensione.
                key = "hls_generic"
                if key not in self.extractors:
                    self.extractors[key] = GenericHLSExtractor(request_headers, proxies=GLOBAL_PROXIES)
                return self.extractors[key]
        except (NameError, TypeError) as e:
            raise ExtractorError(f"Estrattore non disponibile - modulo mancante: {e}")

    async def handle_proxy_request(self, request):
        """Gestisce le richieste proxy principali"""
        if not check_password(request):
            logger.warning(f"⛔ Accesso negato: Password API non valida o mancante. IP: {request.remote}")
            return web.Response(status=401, text="Unauthorized: Invalid API Password")

        
        extractor = None
        try:
            target_url = request.query.get('url') or request.query.get('d')
            force_refresh = request.query.get('force', 'false').lower() == 'true'
            redirect_stream = request.query.get('redirect_stream', 'true').lower() == 'true'
            
            if not target_url:
                return web.Response(text="Parametro 'url' o 'd' mancante", status=400)
            
            try:
                target_url = urllib.parse.unquote(target_url)
            except:
                pass
            # DEBUG LOGGING    
            print(f"🔍 [DEBUG] Processing URL: {target_url}")
            print(f"   Headers: {dict(request.headers)}")
            
            extractor = await self.get_extractor(target_url, dict(request.headers))
            
            print(f"   Extractor: {type(extractor).__name__}")
            
            try:
                # Passa il flag force_refresh all'estrattore
                result = await extractor.extract(target_url, force_refresh=force_refresh)
                stream_url = result["destination_url"]
                stream_headers = result.get("request_headers", {})

                print(f"   Resolved Stream URL: {stream_url}")
                print(f"   Stream Headers: {stream_headers}")
                
                # Se redirect_stream è False, restituisci il JSON con i dettagli (stile MediaFlow)
                if not redirect_stream:
                    # Costruisci l'URL base del proxy
                    scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
                    host = request.headers.get('X-Forwarded-Host', request.host)
                    proxy_base = f"{scheme}://{host}"
                    
                    mediaflow_endpoint = result.get("mediaflow_endpoint", "hls_proxy")
                    
                    # Determina l'endpoint corretto (Logic aggiornata come nell'extractor)
                    endpoint = "/proxy/hls/manifest.m3u8"
                    if mediaflow_endpoint == "proxy_stream_endpoint" or ".mp4" in stream_url or ".mkv" in stream_url or ".avi" in stream_url:
                         endpoint = "/proxy/stream"
                    elif ".mpd" in stream_url:
                        endpoint = "/proxy/mpd/manifest.m3u8"
                        
                    # Prepariamo i parametri per il JSON
                    q_params = {}
                    api_password = request.query.get('api_password')
                    if api_password:
                        q_params['api_password'] = api_password
                    
                    response_data = {
                        "destination_url": stream_url,
                        "request_headers": stream_headers,
                        "mediaflow_endpoint": mediaflow_endpoint,
                        "mediaflow_proxy_url": f"{proxy_base}{endpoint}", # URL Pulito
                        "query_params": q_params
                    }
                    return web.json_response(response_data)

                # Aggiungi headers personalizzati da query params
                h_params_found = []
                for param_name, param_value in request.query.items():
                    if param_name.startswith('h_'):
                        header_name = param_name[2:]
                        h_params_found.append(header_name)
                        
                        # ✅ FIX: Rimuovi eventuali header duplicati (case-insensitive) presenti in stream_headers
                        # Questo assicura che l'header passato via query param (es. h_Referer) abbia la priorità
                        # e non vada in conflitto con quelli generati dagli estrattori (es. referer minuscolo).
                        keys_to_remove = [k for k in stream_headers.keys() if k.lower() == header_name.lower()]
                        for k in keys_to_remove:
                            del stream_headers[k]
                        
                        stream_headers[header_name] = param_value
                
                if h_params_found:
                    logger.debug(f"   Headers overridden by query params: {h_params_found}")
                else:
                    logger.debug("   No h_ params found in query string.")
                    
                # Stream URL resolved
                # ✅ MPD/DASH handling based on MPD_MODE
                if ".mpd" in stream_url or "dash" in stream_url.lower():
                    if MPD_MODE == "ffmpeg" and self.ffmpeg_manager:
                        # FFmpeg transcoding mode
                        logger.info(f"🔄 [FFmpeg Mode] Routing MPD stream: {stream_url}")
                        
                        # Extract ClearKey if present
                        clearkey_param = request.query.get('clearkey')
                        
                        # Support separate key_id and key params (handling multiple keys)
                        if not clearkey_param:
                            key_id_param = request.query.get('key_id')
                            key_val_param = request.query.get('key')
                            
                            if key_id_param and key_val_param:
                                # Check for multiple keys
                                key_ids = key_id_param.split(',')
                                key_vals = key_val_param.split(',')
                                
                                if len(key_ids) == len(key_vals):
                                    clearkey_parts = []
                                    for kid, kval in zip(key_ids, key_vals):
                                        clearkey_parts.append(f"{kid.strip()}:{kval.strip()}")
                                    clearkey_param = ",".join(clearkey_parts)
                                else:
                                    # Fallback or error? defaulting to first or simple concat if mismatch
                                    # Let's try to handle single mismatch case gracefully or just use as is
                                    if len(key_ids) == 1 and len(key_vals) == 1:
                                         clearkey_param = f"{key_id_param}:{key_val_param}"
                                    else:
                                         logger.warning(f"Mismatch in key_id/key count: {len(key_ids)} vs {len(key_vals)}")
                                         # Try to pair as many as possible
                                         min_len = min(len(key_ids), len(key_vals))
                                         clearkey_parts = []
                                         for i in range(min_len):
                                             clearkey_parts.append(f"{key_ids[i].strip()}:{key_vals[i].strip()}")
                                         clearkey_param = ",".join(clearkey_parts)

                            elif key_val_param:
                                clearkey_param = key_val_param
                        
                        playlist_rel_path = await self.ffmpeg_manager.get_stream(stream_url, stream_headers, clearkey=clearkey_param)
                        
                        if playlist_rel_path:
                            # Construct local URL for the FFmpeg stream
                            scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
                            host = request.headers.get('X-Forwarded-Host', request.host)
                            local_url = f"{scheme}://{host}/ffmpeg_stream/{playlist_rel_path}"
                            
                            # Generate Master Playlist for compatibility
                            master_playlist = (
                                "#EXTM3U\n"
                                "#EXT-X-VERSION:3\n"
                                "#EXT-X-STREAM-INF:BANDWIDTH=6000000,NAME=\"Live\"\n"
                                f"{local_url}\n"
                            )
                            
                            return web.Response(
                                text=master_playlist,
                                content_type="application/vnd.apple.mpegurl",
                                headers={
                                    "Access-Control-Allow-Origin": "*",
                                    "Cache-Control": "no-cache"
                                }
                            )
                        else:
                            logger.error("❌ FFmpeg failed to start")
                            return web.Response(text="FFmpeg failed to process stream", status=502)
                    else:
                        # Legacy mode: use mpd_converter for HLS conversion with server-side decryption
                        logger.info(f"🔄 [Legacy Mode] Converting MPD to HLS: {stream_url}")
                        
                        if MPDToHLSConverter is None:
                            logger.error("❌ MPDToHLSConverter not available in legacy mode")
                            return web.Response(text="Legacy MPD converter not available", status=503)
                        
                        # Fetch the MPD manifest
                        session = await self._get_session()
                        
                        # Fix SSL Verification for legacy requests
                        ssl_context = None
                        if get_ssl_setting_for_url(stream_url, TRANSPORT_ROUTES):
                            ssl_context = False

                        async with session.get(stream_url, headers=stream_headers, ssl=ssl_context) as resp:
                            if resp.status != 200:
                                error_text = await resp.text()
                                logger.error(f"❌ Failed to fetch MPD. Status: {resp.status}, URL: {stream_url}")
                                logger.error(f"   Headers: {stream_headers}")
                                logger.error(f"   Response: {error_text[:500]}") # Truncate for safety
                                return web.Response(text=f"Failed to fetch MPD: {resp.status}\nResponse: {error_text[:1000]}", status=502)
                            manifest_content = await resp.text()
                        
                        # Build proxy base URL
                        scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
                        host = request.headers.get('X-Forwarded-Host', request.host)
                        proxy_base = f"{scheme}://{host}"
                        
                        # Build params string with headers
                        params = "".join([f"&h_{urllib.parse.quote(key)}={urllib.parse.quote(value)}" for key, value in stream_headers.items()])
                        
                        # Add api_password if present
                        api_password = request.query.get('api_password')
                        if api_password:
                            params += f"&api_password={api_password}"
                        
                        # Get ClearKey param
                        clearkey_param = request.query.get('clearkey')
                        if not clearkey_param:
                            key_id_param = request.query.get('key_id')
                            key_val_param = request.query.get('key')
                            
                            if key_id_param and key_val_param:
                                # Check for multiple keys
                                key_ids = key_id_param.split(',')
                                key_vals = key_val_param.split(',')
                                
                                if len(key_ids) == len(key_vals):
                                    clearkey_parts = []
                                    for kid, kval in zip(key_ids, key_vals):
                                        clearkey_parts.append(f"{kid.strip()}:{kval.strip()}")
                                    clearkey_param = ",".join(clearkey_parts)
                                else:
                                    if len(key_ids) == 1 and len(key_vals) == 1:
                                         clearkey_param = f"{key_id_param}:{key_val_param}"
                                    else:
                                         logger.warning(f"Mismatch in key_id/key count: {len(key_ids)} vs {len(key_vals)}")
                                         # Try to pair as many as possible
                                         min_len = min(len(key_ids), len(key_vals))
                                         clearkey_parts = []
                                         for i in range(min_len):
                                             clearkey_parts.append(f"{key_ids[i].strip()}:{key_vals[i].strip()}")
                                         clearkey_param = ",".join(clearkey_parts)
                            elif key_val_param:
                                clearkey_param = key_val_param
                        
                        if clearkey_param:
                            params += f"&clearkey={clearkey_param}"
                        
                        # Pass 'ext' param if present (e.g. ext=ts)
                        ext_param = request.query.get('ext')
                        if ext_param:
                            params += f"&ext={ext_param}"
                        
                        # Check if requesting specific representation
                        rep_id = request.query.get('rep_id')
                        
                        converter = MPDToHLSConverter()
                        if rep_id:
                            # Generate media playlist for specific representation
                            hls_content = converter.convert_media_playlist(
                                manifest_content, rep_id, proxy_base, stream_url, params, clearkey_param
                            )
                        else:
                            # Generate master playlist
                            hls_content = converter.convert_master_playlist(
                                manifest_content, proxy_base, stream_url, params
                            )
                        
                        return web.Response(
                            text=hls_content,
                            content_type="application/vnd.apple.mpegurl",
                            headers={
                                "Access-Control-Allow-Origin": "*",
                                "Cache-Control": "no-cache"
                            }
                        )
                
                return await self._proxy_stream(request, stream_url, stream_headers)
            except ExtractorError as e:
                logger.warning(f"Estrazione fallita, tento di nuovo forzando l'aggiornamento: {e}")
                result = await extractor.extract(target_url, force_refresh=True) # Forza sempre il refresh al secondo tentativo
                stream_url = result["destination_url"]
                stream_headers = result.get("request_headers", {})
                # Stream URL resolved after refresh
                return await self._proxy_stream(request, stream_url, stream_headers)
            
        except Exception as e:
            # ✅ MIGLIORATO: Distingui tra errori temporanei (sito offline) ed errori critici
            error_msg = str(e).lower()
            is_temporary_error = any(x in error_msg for x in ['403', 'forbidden', '502', 'bad gateway', 'timeout', 'connection', 'temporarily unavailable'])
            
            extractor_name = "sconosciuto"
            if DLHDExtractor and isinstance(extractor, DLHDExtractor):
                extractor_name = "DLHDExtractor"
            elif VavooExtractor and isinstance(extractor, VavooExtractor):
                extractor_name = "VavooExtractor"

            # Se è un errore temporaneo (sito offline), logga solo un WARNING senza traceback
            if is_temporary_error:
                logger.warning(f"⚠️ {extractor_name}: Servizio temporaneamente non disponibile - {str(e)}")
                return web.Response(text=f"Servizio temporaneamente non disponibile: {str(e)}", status=503)
            
            # Per errori veri (non temporanei), logga come CRITICAL con traceback completo
            logger.critical(f"❌ Errore critico con {extractor_name}: {e}")
            logger.exception(f"Errore nella richiesta proxy: {str(e)}")
            return web.Response(text=f"Errore proxy: {str(e)}", status=500)

    async def handle_extractor_request(self, request):
        """
        Endpoint compatibile con MediaFlow-Proxy per ottenere informazioni sullo stream.
        Supporta redirect_stream per ridirezionare direttamente al proxy.
        """
        # Log request details for debugging
        logger.info(f"📥 Extractor Request: {request.url}")
        
        if not check_password(request):
            logger.warning("⛔ Unauthorized extractor request")
            return web.Response(status=401, text="Unauthorized: Invalid API Password")

        try:
            # Supporta sia 'url' che 'd' come parametro
            url = request.query.get('url') or request.query.get('d')
            if not url:
                # Se non c'è URL, restituisci una pagina di aiuto JSON con gli host disponibili
                help_response = {
                    "message": "EasyProxy Extractor API",
                    "usage": {
                        "endpoint": "/extractor/video",
                        "parameters": {
                            "url": "(Required) URL to extract. Supports plain text, URL encoded, or Base64.",
                            "host": "(Optional) Force specific extractor (bypass auto-detect).",
                            "redirect_stream": "(Optional) 'true' to redirect to stream, 'false' for JSON.",
                            "api_password": "(Optional) API Password if configured."
                        }
                    },
                    "available_hosts": [
                        "vavoo", "dlhd", "daddylive", "vixsrc", "sportsonline", 
                        "mixdrop", "voe", "streamtape", "orion"
                    ],
                    "examples": [
                        f"{request.scheme}://{request.host}/extractor/video?url=https://vavoo.to/channel/123",
                        f"{request.scheme}://{request.host}/extractor/video?host=vavoo&url=https://custom-link.com",
                        f"{request.scheme}://{request.host}/extractor/video?url=BASE64_STRING"
                    ]
                }
                return web.json_response(help_response)

            # Decodifica URL se necessario
            try:
                url = urllib.parse.unquote(url)
            except:
                pass

            # 2. Base64 Decoding (Try)
            try:
                # Tentativo di decodifica Base64 se non sembra un URL valido o se richiesto
                # Aggiunge padding se necessario
                padded_url = url + '=' * (-len(url) % 4)
                decoded_bytes = base64.b64decode(padded_url, validate=True)
                decoded_str = decoded_bytes.decode('utf-8').strip()
                
                # Verifica se il risultato sembra un URL valido
                if decoded_str.startswith('http://') or decoded_str.startswith('https://'):
                    url = decoded_str
                    logger.info(f"🔓 URL Base64 decodificato: {url}")
            except Exception:
                # Non è Base64 o non è un URL valido, proseguiamo con l'originale
                pass
                
            host_param = request.query.get('host')
            redirect_stream = request.query.get('redirect_stream', 'false').lower() == 'true'
            logger.info(f"🔍 Extracting: {url} (Host: {host_param}, Redirect: {redirect_stream})")

            extractor = await self.get_extractor(url, dict(request.headers), host=host_param)
            result = await extractor.extract(url)
            
            stream_url = result["destination_url"]
            stream_headers = result.get("request_headers", {})
            mediaflow_endpoint = result.get("mediaflow_endpoint", "hls_proxy")
            
            logger.info(f"✅ Extraction success: {stream_url[:50]}... Endpoint: {mediaflow_endpoint}")

            # Costruisci l'URL del proxy per questo stream
            scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
            host = request.headers.get('X-Forwarded-Host', request.host)
            proxy_base = f"{scheme}://{host}"
            
            # Determina l'endpoint corretto
            endpoint = "/proxy/hls/manifest.m3u8"
            if mediaflow_endpoint == "proxy_stream_endpoint" or ".mp4" in stream_url or ".mkv" in stream_url or ".avi" in stream_url:
                 endpoint = "/proxy/stream"
            elif ".mpd" in stream_url:
                endpoint = "/proxy/mpd/manifest.m3u8"

            encoded_url = urllib.parse.quote(stream_url, safe='')
            header_params = "".join([f"&h_{urllib.parse.quote(key)}={urllib.parse.quote(value)}" for key, value in stream_headers.items()])
            
            # Aggiungi api_password se presente
            api_password = request.query.get('api_password')
            if api_password:
                header_params += f"&api_password={api_password}"

            # 1. URL COMPLETO (Solo per il redirect)
            full_proxy_url = f"{proxy_base}{endpoint}?d={encoded_url}{header_params}"

            if redirect_stream:
                logger.info(f"↪️ Redirecting to: {full_proxy_url}")
                return web.HTTPFound(full_proxy_url)

            # 2. URL PULITO (Per il JSON stile MediaFlow)
            q_params = {}
            if api_password:
                q_params['api_password'] = api_password

            response_data = {
                "destination_url": stream_url,
                "request_headers": stream_headers,
                "mediaflow_endpoint": mediaflow_endpoint,
                "mediaflow_proxy_url": f"{proxy_base}{endpoint}",
                "query_params": q_params
            }
            
            logger.info(f"✅ Extractor OK: {url} -> {stream_url[:50]}...")
            return web.json_response(response_data)

        except Exception as e:
            error_message = str(e).lower()
            # Per errori attesi (video non trovato, servizio non disponibile), non stampare il traceback
            is_expected_error = any(x in error_message for x in [
                'not found', 'unavailable', '403', 'forbidden', 
                '502', 'bad gateway', 'timeout', 'temporarily unavailable'
            ])
            
            if is_expected_error:
                logger.warning(f"⚠️ Extractor request failed (expected error): {e}")
            else:
                logger.error(f"❌ Error in extractor request: {e}")
                import traceback
                traceback.print_exc()
            
            return web.Response(text=str(e), status=500)

    async def handle_license_request(self, request):
        """✅ NUOVO: Gestisce le richieste di licenza DRM (ClearKey e Proxy)"""
        try:
            # 1. Modalità ClearKey Statica
            clearkey_param = request.query.get('clearkey')
            if clearkey_param:
                logger.info(f"🔑 Richiesta licenza ClearKey statica: {clearkey_param}")
                try:
                    # Support multiple keys separated by comma
                    # Format: KID1:KEY1,KID2:KEY2
                    key_pairs = clearkey_param.split(',')
                    keys_jwk = []
                    
                    # Helper per convertire hex in base64url
                    def hex_to_b64url(hex_str):
                        return base64.urlsafe_b64encode(binascii.unhexlify(hex_str)).decode('utf-8').rstrip('=')

                    for pair in key_pairs:
                        if ':' in pair:
                            kid_hex, key_hex = pair.split(':')
                            keys_jwk.append({
                                "kty": "oct",
                                "k": hex_to_b64url(key_hex),
                                "kid": hex_to_b64url(kid_hex),
                                "type": "temporary"
                            })
                    
                    if not keys_jwk:
                        raise ValueError("No valid keys found")

                    jwk_response = {
                        "keys": keys_jwk,
                        "type": "temporary"
                    }
                    
                    logger.info(f"🔑 Serving static ClearKey license with {len(keys_jwk)} keys")
                    return web.json_response(jwk_response)
                except Exception as e:
                    logger.error(f"❌ Errore nella generazione della licenza ClearKey statica: {e}")
                    return web.Response(text="Invalid ClearKey format", status=400)

            # 2. Modalità Proxy Licenza
            license_url = request.query.get('url')
            if not license_url:
                return web.Response(text="Missing url parameter", status=400)

            license_url = urllib.parse.unquote(license_url)
            
            # Ricostruisce gli headers
            headers = {}
            for param_name, param_value in request.query.items():
                if param_name.startswith('h_'):
                    header_name = param_name[2:].replace('_', '-')
                    headers[header_name] = param_value

            # Aggiunge headers specifici della richiesta originale (es. content-type per il body)
            if request.headers.get('Content-Type'):
                headers['Content-Type'] = request.headers.get('Content-Type')

            # Legge il body della richiesta (challenge DRM)
            body = await request.read()
            
            logger.info(f"🔐 Proxying License Request to: {license_url}")
            
            proxy = random.choice(GLOBAL_PROXIES) if GLOBAL_PROXIES else None
            connector_kwargs = {}
            if proxy:
                connector_kwargs['proxy'] = proxy
            
            async with ClientSession() as session:
                async with session.request(
                    request.method, 
                    license_url, 
                    headers=headers, 
                    data=body, 
                    **connector_kwargs
                ) as resp:
                    response_body = await resp.read()
                    logger.info(f"✅ License response: {resp.status} ({len(response_body)} bytes)")
                    
                    response_headers = {
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Headers": "*",
                        "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
                    }
                    # Copia alcuni headers utili dalla risposta originale
                    if 'Content-Type' in resp.headers:
                        response_headers['Content-Type'] = resp.headers['Content-Type']

                    return web.Response(
                        body=response_body,
                        status=resp.status,
                        headers=response_headers
                    )

        except Exception as e:
            logger.error(f"❌ License proxy error: {str(e)}")
            return web.Response(text=f"License error: {str(e)}", status=500)

    async def handle_key_request(self, request):
        """✅ NUOVO: Gestisce richieste per chiavi AES-128"""
        if not check_password(request):
            return web.Response(status=401, text="Unauthorized: Invalid API Password")

        # 1. Gestione chiave statica (da MPD converter)
        static_key = request.query.get('static_key')
        if static_key:
            try:
                key_bytes = binascii.unhexlify(static_key)
                return web.Response(
                    body=key_bytes,
                    content_type='application/octet-stream',
                    headers={'Access-Control-Allow-Origin': '*'}
                )
            except Exception as e:
                logger.error(f"❌ Errore decodifica chiave statica: {e}")
                return web.Response(text="Invalid static key", status=400)

        # 2. Gestione proxy chiave remota
        key_url = request.query.get('key_url')
        
        if not key_url:
            return web.Response(text="Missing key_url or static_key parameter", status=400)
        
        try:
            # Decodifica l'URL se necessario
            try:
                key_url = urllib.parse.unquote(key_url)
            except:
                pass
                
            # Inizializza gli header esclusivamente da quelli passati dinamicamente
            headers = {}
            for param_name, param_value in request.query.items():
                if param_name.startswith('h_'):
                    header_name = param_name[2:].replace('_', '-')
                    # ✅ FIX: Rimuovi header Range per le richieste di chiavi.
                    if header_name.lower() == 'range':
                        continue
                    headers[header_name] = param_value

            logger.info(f"🔑 Fetching AES key from: {key_url}")
            logger.debug(f"   -> with headers: {headers}")
            
            # ✅ NUOVO: Usa il sistema di routing basato su TRANSPORT_ROUTES
            proxy = get_proxy_for_url(key_url, TRANSPORT_ROUTES, GLOBAL_PROXIES)
            connector_kwargs = {}
            if proxy:
                connector_kwargs['proxy'] = proxy
                logger.info(f"Utilizzo del proxy {proxy} per la richiesta della chiave.")
            
            timeout = ClientTimeout(total=30)
            async with ClientSession(timeout=timeout) as session:
                async with session.get(key_url, headers=headers, **connector_kwargs) as resp:
                    if resp.status == 200 or resp.status == 206:
                        key_data = await resp.read()
                        logger.info(f"✅ AES key fetched successfully: {len(key_data)} bytes")
                        
                        return web.Response(
                            body=key_data,
                            content_type="application/octet-stream",
                            headers={
                                "Access-Control-Allow-Origin": "*",
                                "Access-Control-Allow-Headers": "*",
                                "Cache-Control": "no-cache, no-store, must-revalidate"
                            }
                        )
                    else:
                        logger.error(f"❌ Key fetch failed with status: {resp.status}")
                        # --- LOGICA DI INVALIDAZIONE AUTOMATICA ---
                        try:
                            url_param = request.query.get('original_channel_url')
                            if url_param:
                                extractor = await self.get_extractor(url_param, {})
                                if hasattr(extractor, 'invalidate_cache_for_url'):
                                    await extractor.invalidate_cache_for_url(url_param)
                        except Exception as cache_e:
                            logger.error(f"⚠️ Errore durante l'invalidazione automatica della cache: {cache_e}")
                        # --- FINE LOGICA ---
                        return web.Response(text=f"Key fetch failed: {resp.status}", status=resp.status)
                        
        except Exception as e:
            logger.error(f"❌ Error fetching AES key: {str(e)}")
            return web.Response(text=f"Key error: {str(e)}", status=500)

    async def handle_ts_segment(self, request):
        """Gestisce richieste per segmenti .ts"""
        try:
            segment_name = request.match_info.get('segment')
            base_url = request.query.get('base_url')
            
            if not base_url:
                return web.Response(text="Base URL mancante per segmento", status=400)
            
            base_url = urllib.parse.unquote(base_url)
            
            if base_url.endswith('/'):
                segment_url = f"{base_url}{segment_name}"
            else:
                # ✅ CORREZIONE: Se base_url è un URL completo (es. generato dal converter), usalo direttamente.
                if any(ext in base_url for ext in ['.mp4', '.m4s', '.ts', '.m4i', '.m4a', '.m4v']):
                    segment_url = base_url
                else:
                    segment_url = f"{base_url.rsplit('/', 1)[0]}/{segment_name}"
            
            logger.info(f"📦 Proxy Segment: {segment_name}")
            
            # Gestisce la risposta del proxy per il segmento
            return await self._proxy_segment(request, segment_url, {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "referer": base_url
            }, segment_name)
            
        except Exception as e:
            logger.error(f"Errore nel proxy segmento .ts: {str(e)}")
            return web.Response(text=f"Errore segmento: {str(e)}", status=500)

    async def _proxy_segment(self, request, segment_url, stream_headers, segment_name):
        """✅ NUOVO: Proxy dedicato per segmenti .ts con Content-Disposition"""
        try:
            headers = dict(stream_headers)
            
            # Passa attraverso alcuni headers del client
            for header in ['range', 'if-none-match', 'if-modified-since']:
                if header in request.headers:
                    headers[header] = request.headers[header]
            
            proxy = random.choice(GLOBAL_PROXIES) if GLOBAL_PROXIES else None
            connector_kwargs = {}
            if proxy:
                connector_kwargs['proxy'] = proxy
                logger.debug(f"📡 [Proxy Segment] Utilizzo del proxy {proxy} per il segmento .ts")

            timeout = ClientTimeout(total=60, connect=30)
            async with ClientSession(timeout=timeout) as session:
                async with session.get(segment_url, headers=headers, **connector_kwargs) as resp:
                    response_headers = {}
                    
                    for header in ['content-type', 'content-length', 'content-range', 
                                 'accept-ranges', 'last-modified', 'etag']:
                        if header in resp.headers:
                            response_headers[header] = resp.headers[header]
                    
                    # Forza il content-type e aggiunge Content-Disposition per .ts
                    response_headers['Content-Type'] = 'video/MP2T'
                    response_headers['Content-Disposition'] = f'attachment; filename="{segment_name}"'
                    response_headers['Access-Control-Allow-Origin'] = '*'
                    response_headers['Access-Control-Allow-Methods'] = 'GET, HEAD, OPTIONS'
                    response_headers['Access-Control-Allow-Headers'] = 'Range, Content-Type'
                    
                    response = web.StreamResponse(
                        status=resp.status,
                        headers=response_headers
                    )
                    
                    await response.prepare(request)
                    
                    async for chunk in resp.content.iter_chunked(8192):
                        await response.write(chunk)
                    
                    await response.write_eof()
                    return response
                    
        except Exception as e:
            logger.error(f"Errore nel proxy del segmento: {str(e)}")
            return web.Response(text=f"Errore segmento: {str(e)}", status=500)

    async def _proxy_stream(self, request, stream_url, stream_headers):
        """Effettua il proxy dello stream con gestione manifest e AES-128"""
        try:
            headers = dict(stream_headers)
            
            # Passa attraverso alcuni headers del client, ma FILTRA quelli che potrebbero leakare l'IP
            for header in ['range', 'if-none-match', 'if-modified-since']:
                if header in request.headers:
                    headers[header] = request.headers[header]
            
            # Rimuovi esplicitamente headers che potrebbero rivelare l'IP originale
            for h in ["x-forwarded-for", "x-real-ip", "forwarded", "via"]:
                if h in headers:
                    del headers[h]
            
            proxy = random.choice(GLOBAL_PROXIES) if GLOBAL_PROXIES else None
            connector_kwargs = {}
            if proxy:
                connector_kwargs['proxy'] = proxy
                logger.info(f"📡 [Proxy Stream] Utilizzo del proxy {proxy} per la richiesta verso: {stream_url}")

            # ✅ FIX: Normalizza gli header critici (User-Agent, Referer) in Title-Case
            for key in list(headers.keys()):
                if key.lower() == 'user-agent':
                    headers['User-Agent'] = headers.pop(key)
                elif key.lower() == 'referer':
                    headers['Referer'] = headers.pop(key)
                elif key.lower() == 'origin':
                    headers['Origin'] = headers.pop(key)
                elif key.lower() == 'authorization':
                    headers['Authorization'] = headers.pop(key)
                elif key.lower() == 'cookie':
                    headers['Cookie'] = headers.pop(key)

            # ✅ FIX: Rimuovi duplicati espliciti se presenti (es. user-agent e User-Agent)
            # Questo può accadere se GenericHLSExtractor aggiunge 'user-agent' e noi abbiamo 'User-Agent' da h_ params
            # La normalizzazione sopra dovrebbe averli unificati, ma per sicurezza puliamo.
            
            # Log headers finali per debug
            # logger.info(f"   Final Stream Headers: {headers}")

            # ✅ NUOVO: Determina se disabilitare SSL per questo dominio
            disable_ssl = get_ssl_setting_for_url(stream_url, TRANSPORT_ROUTES)

            timeout = ClientTimeout(total=60, connect=30)
            async with ClientSession(timeout=timeout) as session:
                async with session.get(stream_url, headers=headers, **connector_kwargs, ssl=not disable_ssl) as resp:
                    content_type = resp.headers.get('content-type', '')
                    
                    print(f"   Upstream Response: {resp.status} [{content_type}]")

                    # ✅ FIX: Se la risposta non è OK, restituisci direttamente l'errore senza processare
                    if resp.status not in [200, 206]:
                        logger.warning(f"⚠️ Upstream returned error {resp.status} for {stream_url}")
                        return web.Response(
                            body=await resp.read(),
                            status=resp.status,
                            headers={
                                'Content-Type': content_type,
                                'Access-Control-Allow-Origin': '*'
                            }
                        )
                    
                    # Gestione special per manifest HLS
                    # ✅ CORREZIONE: Gestisce anche i manifest mascherati da .css (usati da DLHD)
                    if 'mpegurl' in content_type or stream_url.endswith('.m3u8') or (stream_url.endswith('.css') and 'newkso.ru' in stream_url):
                        manifest_content = await resp.text()
                        
                        # ✅ CORREZIONE: Rileva lo schema e l'host corretti quando dietro un reverse proxy
                        scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
                        host = request.headers.get('X-Forwarded-Host', request.host)
                        proxy_base = f"{scheme}://{host}"
                        original_channel_url = request.query.get('url', '')
                        
                        api_password = request.query.get('api_password')
                        rewritten_manifest = await ManifestRewriter.rewrite_manifest_urls(
                            manifest_content, stream_url, proxy_base, headers, original_channel_url, api_password, self.get_extractor
                        )
                        
                        return web.Response(
                            text=rewritten_manifest,
                            headers={
                                'Content-Type': 'application/vnd.apple.mpegurl',
                                'Content-Disposition': 'attachment; filename="stream.m3u8"',
                                'Access-Control-Allow-Origin': '*',
                                'Cache-Control': 'no-cache'
                            }
                        )
                    
                    # ✅ AGGIORNATO: Gestione per manifest MPD (DASH)
                    elif 'dash+xml' in content_type or stream_url.endswith('.mpd'):
                        manifest_content = await resp.text()
                        
                        # ✅ CORREZIONE: Rileva lo schema e l'host corretti quando dietro un reverse proxy
                        scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
                        host = request.headers.get('X-Forwarded-Host', request.host)
                        proxy_base = f"{scheme}://{host}"
                        
                        # Recupera parametri
                        clearkey_param = request.query.get('clearkey')
                        
                        # ✅ FIX: Supporto per key_id e key separati (stile MediaFlowProxy)
                        if not clearkey_param:
                            key_id_param = request.query.get('key_id')
                            key_val_param = request.query.get('key')
                            
                            if key_id_param and key_val_param:
                                # Check for multiple keys
                                key_ids = key_id_param.split(',')
                                key_vals = key_val_param.split(',')
                                
                                if len(key_ids) == len(key_vals):
                                    clearkey_parts = []
                                    for kid, kval in zip(key_ids, key_vals):
                                        clearkey_parts.append(f"{kid.strip()}:{kval.strip()}")
                                    clearkey_param = ",".join(clearkey_parts)
                                else:
                                    if len(key_ids) == 1 and len(key_vals) == 1:
                                         clearkey_param = f"{key_id_param}:{key_val_param}"
                                    else:
                                         # Try to pair as many as possible
                                         min_len = min(len(key_ids), len(key_vals))
                                         clearkey_parts = []
                                         for i in range(min_len):
                                             clearkey_parts.append(f"{key_ids[i].strip()}:{key_vals[i].strip()}")
                                         clearkey_param = ",".join(clearkey_parts)

                        # --- LEGACY MODE: MPD -> HLS Conversion ---
                        if MPD_MODE == "legacy" and MPDToHLSConverter:
                            logger.info(f"🔄 [Legacy Mode] Converting MPD to HLS for {stream_url}")
                            try:
                                converter = MPDToHLSConverter()
                                
                                # Check if requesting a Media Playlist (Variant)
                                rep_id = request.query.get('rep_id')
                                
                                if rep_id:
                                    # Generate Media Playlist (Segments)
                                    hls_playlist = converter.convert_media_playlist(
                                        manifest_content, rep_id, proxy_base, stream_url, request.query_string, clearkey_param
                                    )
                                    # Log first few lines for debugging
                                    logger.info(f"📜 Generated Media Playlist for {rep_id} (first 10 lines):\n{chr(10).join(hls_playlist.splitlines()[:10])}")
                                else:
                                    # Generate Master Playlist
                                    hls_playlist = converter.convert_master_playlist(
                                        manifest_content, proxy_base, stream_url, request.query_string
                                    )
                                    logger.info(f"📜 Generated Master Playlist (first 5 lines):\n{chr(10).join(hls_playlist.splitlines()[:5])}")
                                
                                return web.Response(
                                    text=hls_playlist,
                                    headers={
                                        'Content-Type': 'application/vnd.apple.mpegurl',
                                        'Content-Disposition': 'attachment; filename="stream.m3u8"',
                                        'Access-Control-Allow-Origin': '*',
                                        'Cache-Control': 'no-cache'
                                    }
                                )
                            except Exception as e:
                                logger.error(f"❌ Legacy conversion failed: {e}")
                                # Fallback to DASH proxy if conversion fails
                                pass

                        # --- DEFAULT: DASH Proxy (Rewriting) ---
                        req_format = request.query.get('format')
                        rep_id = request.query.get('rep_id')

                        api_password = request.query.get('api_password')
                        rewritten_manifest = ManifestRewriter.rewrite_mpd_manifest(manifest_content, stream_url, proxy_base, headers, clearkey_param, api_password)
                        
                        return web.Response(
                            text=rewritten_manifest,
                            headers={
                                'Content-Type': 'application/dash+xml',
                                'Content-Disposition': 'attachment; filename="stream.mpd"',
                                'Access-Control-Allow-Origin': '*',
                                'Cache-Control': 'no-cache'
                            })
                    
                    # Streaming normale per altri tipi di contenuto
                    response_headers = {}
                    
                    for header in ['content-type', 'content-length', 'content-range', 
                                 'accept-ranges', 'last-modified', 'etag']:
                        if header in resp.headers:
                            response_headers[header] = resp.headers[header]
                    
                    # ✅ FIX: Forza Content-Type per segmenti .ts se il server non lo invia correttamente
                    if (stream_url.endswith('.ts') or request.path.endswith('.ts')) and 'video/mp2t' not in response_headers.get('content-type', '').lower():
                        response_headers['Content-Type'] = 'video/MP2T'

                    response_headers['Access-Control-Allow-Origin'] = '*'
                    response_headers['Access-Control-Allow-Methods'] = 'GET, HEAD, OPTIONS'
                    response_headers['Access-Control-Allow-Headers'] = 'Range, Content-Type'
                    
                    response = web.StreamResponse(
                        status=resp.status,
                        headers=response_headers
                    )
                    
                    await response.prepare(request)
                    
                    async for chunk in resp.content.iter_chunked(8192):
                        await response.write(chunk)
                    
                    await response.write_eof()
                    return response
                    
        except (ClientPayloadError, ConnectionResetError, OSError) as e:
            # Errori tipici di disconnessione del client
            logger.info(f"ℹ️ Client disconnesso dallo stream: {stream_url} ({str(e)})")
            return web.Response(text="Client disconnected", status=499)
            
        except (ServerDisconnectedError, ClientConnectionError, asyncio.TimeoutError) as e:
            # Errori di connessione upstream
            logger.warning(f"⚠️ Connessione persa con la sorgente: {stream_url} ({str(e)})")
            return web.Response(text=f"Upstream connection lost: {str(e)}", status=502)

        except Exception as e:
            logger.error(f"❌ Errore generico nel proxy dello stream: {str(e)}")
            return web.Response(text=f"Errore stream: {str(e)}", status=500)

    async def handle_playlist_request(self, request):
        """Gestisce le richieste per il playlist builder"""
        if not self.playlist_builder:
            return web.Response(text="❌ Playlist Builder non disponibile - modulo mancante", status=503)
            
        try:
            url_param = request.query.get('url')
            
            if not url_param:
                return web.Response(text="Parametro 'url' mancante", status=400)
            
            if not url_param.strip():
                return web.Response(text="Parametro 'url' non può essere vuoto", status=400)
            
            playlist_definitions = [def_.strip() for def_ in url_param.split(';') if def_.strip()]
            if not playlist_definitions:
                return web.Response(text="Nessuna definizione playlist valida trovata", status=400)
            
            # ✅ CORREZIONE: Rileva lo schema e l'host corretti quando dietro un reverse proxy
            scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
            host = request.headers.get('X-Forwarded-Host', request.host)
            base_url = f"{scheme}://{host}"
            
            # ✅ FIX: Passa api_password al builder se presente
            api_password = request.query.get('api_password')
            
            async def generate_response():
                async for line in self.playlist_builder.async_generate_combined_playlist(
                    playlist_definitions, base_url, api_password=api_password
                ):
                    yield line.encode('utf-8')
            
            response = web.StreamResponse(
                status=200,
                headers={
                    'Content-Type': 'application/vnd.apple.mpegurl',
                    'Content-Disposition': 'attachment; filename="playlist.m3u"',
                    'Access-Control-Allow-Origin': '*'
                }
            )
            
            await response.prepare(request)
            
            async for chunk in generate_response():
                await response.write(chunk)
            
            await response.write_eof()
            return response
            
        except Exception as e:
            logger.error(f"Errore generale nel playlist handler: {str(e)}")
            return web.Response(text=f"Errore: {str(e)}", status=500)

    def _read_template(self, filename: str) -> str:
        """Funzione helper per leggere un file di template."""
        # Nota: assume che i template siano nella directory 'templates' nella root del progetto
        # Poiché siamo in services/, dobbiamo salire di un livello
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        template_path = os.path.join(base_dir, 'templates', filename)
        with open(template_path, 'r', encoding='utf-8') as f:
            return f.read()

    async def handle_root(self, request):
        """Serve la pagina principale index.html."""
        try:
            html_content = self._read_template('index.html')
            return web.Response(text=html_content, content_type='text/html')
        except Exception as e:
            logger.error(f"❌ Errore critico: impossibile caricare 'index.html': {e}")
            return web.Response(text="<h1>Errore 500</h1><p>Pagina non trovata.</p>", status=500, content_type='text/html')

    async def handle_builder(self, request):
        """Gestisce l'interfaccia web del playlist builder."""
        try:
            html_content = self._read_template('builder.html')
            return web.Response(text=html_content, content_type='text/html')
        except Exception as e:
            logger.error(f"❌ Errore critico: impossibile caricare 'builder.html': {e}")
            return web.Response(text="<h1>Errore 500</h1><p>Impossibile caricare l'interfaccia builder.</p>", status=500, content_type='text/html')

    async def handle_info_page(self, request):
        """Serve la pagina HTML delle informazioni."""
        try:
            html_content = self._read_template('info.html')
            return web.Response(text=html_content, content_type='text/html')
        except Exception as e:
            logger.error(f"❌ Errore critico: impossibile caricare 'info.html': {e}")
            return web.Response(text="<h1>Errore 500</h1><p>Impossibile caricare la pagina info.</p>", status=500, content_type='text/html')

    async def handle_favicon(self, request):
        """Serve il file favicon.ico."""
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        favicon_path = os.path.join(base_dir, 'static', 'favicon.ico')
        if os.path.exists(favicon_path):
            return web.FileResponse(favicon_path)
        return web.Response(status=404)

    async def handle_options(self, request):
        """Gestisce richieste OPTIONS per CORS"""
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
            'Access-Control-Allow-Headers': 'Range, Content-Type',
            'Access-Control-Max-Age': '86400'
        }
        return web.Response(headers=headers)

    async def handle_api_info(self, request):
        """Endpoint API che restituisce le informazioni sul server in formato JSON."""
        info = {
            "proxy": "HLS Proxy Server",
            "version": "2.5.0",  # Aggiornata per supporto AES-128
            "status": "✅ Funzionante",
            "features": [
                "✅ Proxy HLS streams",
                "✅ AES-128 key proxying",  # ✅ NUOVO
                "✅ Playlist building",
                "✅ Supporto Proxy (SOCKS5, HTTP/S)",
                "✅ Multi-extractor support",
                "✅ CORS enabled"
            ],
            "extractors_loaded": list(self.extractors.keys()),
            "modules": {
                "playlist_builder": PlaylistBuilder is not None,
                "vavoo_extractor": VavooExtractor is not None,
                "dlhd_extractor": DLHDExtractor is not None,
                "vixsrc_extractor": VixSrcExtractor is not None,
                "sportsonline_extractor": SportsonlineExtractor is not None,
                "mixdrop_extractor": MixdropExtractor is not None,
                "voe_extractor": VoeExtractor is not None,
                "streamtape_extractor": StreamtapeExtractor is not None,
            },
            "proxy_config": {
                "global_proxies": f"{len(GLOBAL_PROXIES)} proxies caricati",
                "transport_routes": f"{len(TRANSPORT_ROUTES)} regole di routing configurate",
                "routes": [{"url": route['url'], "has_proxy": route['proxy'] is not None} for route in TRANSPORT_ROUTES]
            },
            "endpoints": {
                "/proxy/hls/manifest.m3u8": "Proxy HLS (compatibilità MFP) - ?d=<URL>",
                "/proxy/mpd/manifest.m3u8": "Proxy MPD (compatibilità MFP) - ?d=<URL>",
                "/proxy/manifest.m3u8": "Proxy Legacy - ?url=<URL>",
                "/key": "Proxy chiavi AES-128 - ?key_url=<URL>",  # ✅ NUOVO
                "/playlist": "Playlist builder - ?url=<definizioni>",
                "/builder": "Interfaccia web per playlist builder",
                "/segment/{segment}": "Proxy per segmenti .ts - ?base_url=<URL>",
                "/license": "Proxy licenze DRM (ClearKey/Widevine) - ?url=<URL> o ?clearkey=<id:key>",
                "/info": "Pagina HTML con informazioni sul server",
                "/api/info": "Endpoint JSON con informazioni sul server"
            },
            "usage_examples": {
                "proxy_hls": "/proxy/hls/manifest.m3u8?d=https://example.com/stream.m3u8",
                "proxy_mpd": "/proxy/mpd/manifest.m3u8?d=https://example.com/stream.mpd",
                "aes_key": "/key?key_url=https://server.com/key.bin",  # ✅ NUOVO
                "playlist": "/playlist?url=http://example.com/playlist1.m3u8;http://example.com/playlist2.m3u8",
                "custom_headers": "/proxy/hls/manifest.m3u8?d=<URL>&h_Authorization=Bearer%20token"
            }
        }
        return web.json_response(info)

    def _prefetch_next_segments(self, current_url, init_url, key, key_id, headers):
        """Identifica i prossimi segmenti e avvia il download in background."""
        try:
            parsed = urllib.parse.urlparse(current_url)
            path = parsed.path
            
            # Cerca pattern numerico alla fine del path (es. segment-1.m4s)
            match = re.search(r'([-_])(\d+)(\.[^.]+)$', path)
            if not match:
                return

            separator, current_number, extension = match.groups()
            current_num = int(current_number)

            # Prefetch next 5 segments (increased from 3 for smoother multi-key playback)
            for i in range(1, 6):
                next_num = current_num + i
                
                # Replace number in path
                pattern = f"{separator}{current_number}{re.escape(extension)}$"
                replacement = f"{separator}{next_num}{extension}"
                new_path = re.sub(pattern, replacement, path)
                
                # Reconstruct URL
                next_url = urllib.parse.urlunparse(parsed._replace(path=new_path))
                
                cache_key = f"{next_url}:{key_id}"
                
                if (cache_key not in self.segment_cache and 
                    cache_key not in self.prefetch_tasks):
                    
                    self.prefetch_tasks.add(cache_key)
                    asyncio.create_task(
                        self._fetch_and_cache_segment(next_url, init_url, key, key_id, headers, cache_key)
                    )

        except Exception as e:
            logger.warning(f"⚠️ Prefetch error: {e}")

    async def _fetch_and_cache_segment(self, url, init_url, key, key_id, headers, cache_key):
        """Scarica, decripta e mette in cache un segmento in background."""
        try:
            if decrypt_segment is None:
                return

            session = await self._get_session()
            
            # Download Init (usa cache se possibile)
            init_content = b""
            if init_url:
                if init_url in self.init_cache:
                    init_content = self.init_cache[init_url]
                else:
                    disable_ssl = get_ssl_setting_for_url(init_url, TRANSPORT_ROUTES)
                    try:
                        async with session.get(init_url, headers=headers, ssl=not disable_ssl, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                            if resp.status == 200:
                                init_content = await resp.read()
                                self.init_cache[init_url] = init_content
                    except Exception:
                        pass 

            # Download Segment
            segment_content = None
            disable_ssl = get_ssl_setting_for_url(url, TRANSPORT_ROUTES)
            try:
                async with session.get(url, headers=headers, ssl=not disable_ssl, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        segment_content = await resp.read()
            except Exception:
                pass

            if segment_content:
                # Decrypt
                # Decrypt in thread pool to avoid blocking event loop
                loop = asyncio.get_event_loop()
                decrypted_content = await loop.run_in_executor(None, decrypt_segment, init_content, segment_content, key_id, key)
                import time
                self.segment_cache[cache_key] = (decrypted_content, time.time())
                logger.info(f"📦 Prefetched segment: {url.split('/')[-1]}")

        except Exception as e:
            pass
        finally:
            if cache_key in self.prefetch_tasks:
                self.prefetch_tasks.remove(cache_key)

    async def _remux_to_ts(self, content):
        """Converte segmenti (fMP4) in MPEG-TS usando FFmpeg pipe."""
        try:
            cmd = [
                'ffmpeg',
                '-y',
                '-i', 'pipe:0',
                '-c', 'copy',
                '-copyts',                      # Preserve timestamps to prevent freezing/gap issues
                '-bsf:v', 'h264_mp4toannexb',   # Ensure video is Annex B (MPEG-TS requirement)
                '-bsf:a', 'aac_adtstoasc',      # Ensure audio is ADTS (MPEG-TS requirement)
                '-f', 'mpegts',
                'pipe:1'
            ]
            
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await proc.communicate(input=content)
            
            # Check for data presence regardless of return code (workaround for asyncio race condition on some platforms)
            if len(stdout) > 0:
                if proc.returncode != 0:
                    logger.warning(f"⚠️ FFmpeg remux finished with code {proc.returncode} but produced output (ignoring error). Stderr: {stderr.decode()[:200]}")
                return stdout
            
            if proc.returncode != 0:
                logger.error(f"❌ FFmpeg remux failed: {stderr.decode()}")
                return None
                
            return stdout
        except Exception as e:
            logger.error(f"❌ Remux error: {e}")
            return None

    async def handle_decrypt_segment(self, request):
        """Decripta segmenti fMP4 lato server per ClearKey (legacy mode)."""
        if not check_password(request):
            return web.Response(status=401, text="Unauthorized: Invalid API Password")

        url = request.query.get('url')
        logger.info(f"🔓 Decrypt Request: {url.split('/')[-1] if url else 'unknown'}")

        init_url = request.query.get('init_url')
        key = request.query.get('key')
        key_id = request.query.get('key_id')
        
        if not url or not key or not key_id:
            return web.Response(text="Missing url, key, or key_id", status=400)

        if decrypt_segment is None:
            return web.Response(text="Decrypt not available (MPD_MODE is not legacy)", status=503)

        # Check cache first
        import time
        cache_key = f"{url}:{key_id}:ts" # Use distinct cache key for TS
        if cache_key in self.segment_cache:
            cached_content, cached_time = self.segment_cache[cache_key]
            if time.time() - cached_time < self.segment_cache_ttl:
                logger.info(f"📦 Cache HIT for segment: {url.split('/')[-1]}")
                return web.Response(
                    body=cached_content,
                    status=200,
                    headers={
                        'Content-Type': 'video/MP2T',
                        'Access-Control-Allow-Origin': '*',
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive'
                    }
                )
            else:
                del self.segment_cache[cache_key]

        try:
            # Ricostruisce gli headers per le richieste upstream
            headers = {
                'Connection': 'keep-alive',
                'Accept-Encoding': 'identity'
            }
            for param_name, param_value in request.query.items():
                if param_name.startswith('h_'):
                    header_name = param_name[2:].replace('_', '-')
                    headers[header_name] = param_value

            session = await self._get_session()

            # Parallel download of init and media segment
            async def fetch_init():
                if not init_url:
                    return b""
                if init_url in self.init_cache:
                    return self.init_cache[init_url]
                disable_ssl = get_ssl_setting_for_url(init_url, TRANSPORT_ROUTES)
                async with session.get(init_url, headers=headers, ssl=not disable_ssl, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        self.init_cache[init_url] = content
                        return content
                    return None

            async def fetch_segment():
                disable_ssl = get_ssl_setting_for_url(url, TRANSPORT_ROUTES)
                async with session.get(url, headers=headers, ssl=not disable_ssl, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    return None

            # Parallel fetch
            init_content, segment_content = await asyncio.gather(fetch_init(), fetch_segment())
            
            if init_content is None and init_url:
                logger.error(f"❌ Failed to fetch init segment")
                return web.Response(status=502)
            if segment_content is None:
                logger.error(f"❌ Failed to fetch segment")
                return web.Response(status=502)

            init_content = init_content or b""

            # Check if we should skip decryption (null key case)
            skip_decrypt = request.query.get('skip_decrypt') == '1'
            
            if skip_decrypt:
                # Null key: just concatenate init + segment without decryption
                logger.info(f"🔓 Skip decrypt mode - remuxing without decryption")
                combined_content = init_content + segment_content
            else:
                # Decripta con PyCryptodome
                # Decrypt in thread pool to avoid blocking event loop
                loop = asyncio.get_event_loop()
                combined_content = await loop.run_in_executor(None, decrypt_segment, init_content, segment_content, key_id, key)

            # Check if we should skip remux (fMP4 -> TS) for faster playback
            # Most modern players (VLC, ffplay, mpv) can handle fMP4 directly
            skip_remux = request.query.get('skip_remux') == '1'
            
            if skip_remux:
                # Serve decrypted fMP4 directly - much faster!
                logger.info("⚡ Skip remux - serving fMP4 directly")
                ts_content = combined_content
                content_type = 'video/mp4'
            else:
                # Legacy: Remux to TS for players that need it (slower)
                ts_content = await self._remux_to_ts(combined_content)
                if not ts_content:
                     logger.warning("⚠️ Remux failed, serving raw fMP4")
                     ts_content = combined_content
                     content_type = 'video/mp4'
                else:
                     content_type = 'video/MP2T'
                     logger.info("⚡ Remuxed fMP4 -> TS")

            # Store in cache
            self.segment_cache[cache_key] = (ts_content, time.time())
            
            # Clean old cache entries (keep max 150 for multi-key streams)
            if len(self.segment_cache) > 150:
                oldest_keys = sorted(self.segment_cache.keys(), key=lambda k: self.segment_cache[k][1])[:50]
                for k in oldest_keys:
                    del self.segment_cache[k]

            # Prefetch next segments in background
            self._prefetch_next_segments(url, init_url, key, key_id, headers)

            # Invia Risposta
            return web.Response(
                body=ts_content,
                status=200,
                headers={
                    'Content-Type': content_type,
                    'Access-Control-Allow-Origin': '*',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive'
                }
            )

        except Exception as e:
            logger.error(f"❌ Decryption error: {e}")
            return web.Response(status=500, text=f"Decryption failed: {str(e)}")

    async def handle_generate_urls(self, request):
        """
        Endpoint compatibile con MediaFlow-Proxy per generare URL proxy.
        Supporta la richiesta POST da ilCorsaroViola.
        """
        try:
            data = await request.json()
            
            # Verifica password se presente nel body (ilCorsaroViola la manda qui)
            req_password = data.get('api_password')
            if API_PASSWORD and req_password != API_PASSWORD:
                 # Fallback: check standard auth methods if body auth fails or is missing
                 if not check_password(request):
                    logger.warning("⛔ Unauthorized generate_urls request")
                    return web.Response(status=401, text="Unauthorized: Invalid API Password")

            urls_to_process = data.get('urls', [])
            
            # --- LOGGING RICHIESTO ---
            client_ip = request.remote
            exit_strategy = "IP del Server (Diretto)"
            if GLOBAL_PROXIES:
                exit_strategy = f"Proxy Globale Random (Pool di {len(GLOBAL_PROXIES)} proxy)"
            
            logger.info(f"🔄 [Generate URLs] Richiesta da Client IP: {client_ip}")
            logger.info(f"    -> Strategia di uscita prevista per lo stream: {exit_strategy}")
            if urls_to_process:
                logger.info(f"    -> Generazione di {len(urls_to_process)} URL proxy per destinazione: {urls_to_process[0].get('destination_url', 'N/A')}")
            # -------------------------

            generated_urls = []
            
            # Determina base URL del proxy
            scheme = request.headers.get('X-Forwarded-Proto', request.scheme)
            host = request.headers.get('X-Forwarded-Host', request.host)
            proxy_base = f"{scheme}://{host}"

            for item in urls_to_process:
                dest_url = item.get('destination_url')
                if not dest_url:
                    continue
                    
                endpoint = item.get('endpoint', '/proxy/stream')
                req_headers = item.get('request_headers', {})
                
                # Costruisci query params
                encoded_url = urllib.parse.quote(dest_url, safe='')
                params = [f"d={encoded_url}"]
                
                # Aggiungi headers come h_ params
                for key, value in req_headers.items():
                    params.append(f"h_{urllib.parse.quote(key)}={urllib.parse.quote(value)}")
                
                # Aggiungi password se necessaria
                if API_PASSWORD:
                    params.append(f"api_password={API_PASSWORD}")
                
                # Costruisci URL finale
                query_string = "&".join(params)
                
                # Assicuriamoci che l'endpoint inizi con /
                if not endpoint.startswith('/'):
                    endpoint = '/' + endpoint
                
                full_url = f"{proxy_base}{endpoint}?{query_string}"
                generated_urls.append(full_url)

            return web.json_response({"urls": generated_urls})

        except Exception as e:
            logger.error(f"❌ Error generating URLs: {e}")
            return web.Response(text=str(e), status=500)

    async def handle_proxy_ip(self, request):
        """Restituisce l'indirizzo IP pubblico del server (o del proxy se configurato)."""
        if not check_password(request):
            return web.Response(status=401, text="Unauthorized: Invalid API Password")

        try:
            # Usa un proxy globale se configurato, altrimenti connessione diretta
            proxy = random.choice(GLOBAL_PROXIES) if GLOBAL_PROXIES else None
            
            # Crea una sessione dedicata con il proxy configurato
            if proxy:
                logger.info(f"🌍 Checking IP via proxy: {proxy}")
                connector = ProxyConnector.from_url(proxy)
            else:
                connector = TCPConnector()
            
            timeout = ClientTimeout(total=10)
            async with ClientSession(timeout=timeout, connector=connector) as session:
                # Usa un servizio esterno per determinare l'IP pubblico
                async with session.get('https://api.ipify.org?format=json') as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return web.json_response(data)
                    else:
                        logger.error(f"❌ Failed to fetch IP: {resp.status}")
                        return web.Response(text="Failed to fetch IP", status=502)
                    
        except Exception as e:
            logger.error(f"❌ Error fetching IP: {e}")
            return web.Response(text=str(e), status=500)

    async def cleanup(self):
        """Pulizia delle risorse"""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                
            for extractor in self.extractors.values():
                if hasattr(extractor, 'close'):
                    await extractor.close()
        except Exception as e:
            logger.error(f"Errore durante cleanup: {e}")
