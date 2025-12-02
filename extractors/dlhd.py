import asyncio
import logging
import re
import base64
import json
import os
import gzip
import zlib
import zstandard
import random
from urllib.parse import urlparse, quote_plus
import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector, FormData
from aiohttp_proxy import ProxyConnector
from typing import Dict, Any, Optional
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class ExtractorError(Exception):
    pass

class DLHDExtractor:
    """DLHD Extractor con sessione persistente e gestione anti-bot avanzata"""

    def __init__(self, request_headers: dict, proxies: list = None):
        self.request_headers = request_headers
        self.base_headers = {
            # ✅ User-Agent più recente per bypassare protezioni anti-bot
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        }
        self.session = None
        self.mediaflow_endpoint = "hls_manifest_proxy"
        self._cached_base_url = None
        self._iframe_context = None
        self._session_lock = asyncio.Lock()
        self.proxies = proxies or []
        self._extraction_locks: Dict[str, asyncio.Lock] = {} # ✅ NUOVO: Lock per evitare estrazioni multiple
        self.cache_file = os.path.join(os.path.dirname(__file__), '.dlhd_cache')
        self._stream_data_cache: Dict[str, Dict[str, Any]] = self._load_cache()

    def _load_cache(self) -> Dict[str, Dict[str, Any]]:
        """Carica la cache da un file codificato in Base64 all'avvio."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    logger.info(f"💾 Caricamento cache dal file: {self.cache_file}")
                    encoded_data = f.read()
                    if not encoded_data:
                        return {}
                    decoded_data = base64.b64decode(encoded_data).decode('utf-8')
                    return json.loads(decoded_data)
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"❌ Errore durante il caricamento della cache: {e}. Inizio con una cache vuota.")
        return {}

    def _get_random_proxy(self):
        """Restituisce un proxy casuale dalla lista."""
        return random.choice(self.proxies) if self.proxies else None

    async def _get_session(self):
        """✅ Sessione persistente con cookie jar automatico"""
        if self.session is None or self.session.closed:
            timeout = ClientTimeout(total=60, connect=30, sock_read=30)
            proxy = self._get_random_proxy()
            if proxy:
                logger.info(f"🔗 Utilizzo del proxy {proxy} per la sessione DLHD.")
                connector = ProxyConnector.from_url(proxy, ssl=False)
            else:
                connector = TCPConnector(
                    limit=10,
                    limit_per_host=3,
                    keepalive_timeout=30,
                    enable_cleanup_closed=True,
                    force_close=False,
                    use_dns_cache=True
                )
                logger.info("ℹ️ Nessun proxy specifico per DLHD, uso connessione diretta.")
            # ✅ FONDAMENTALE: Cookie jar per mantenere sessione come browser reale
            self.session = ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self.base_headers,
                cookie_jar=aiohttp.CookieJar()
            )
        return self.session

    def _save_cache(self):
        """Salva lo stato corrente della cache su un file, codificando il contenuto in Base64."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json_data = json.dumps(self._stream_data_cache)
                encoded_data = base64.b64encode(json_data.encode('utf-8')).decode('utf-8')
                f.write(encoded_data)
                logger.info(f"💾 Cache codificata e salvata con successo nel file: {self.cache_file}")
        except IOError as e:
            logger.error(f"❌ Errore durante il salvataggio della cache: {e}")

    def _get_headers_for_url(self, url: str, base_headers: dict) -> dict:
        """Applica headers specifici per newkso.ru automaticamente"""
        headers = base_headers.copy()
        parsed_url = urlparse(url)
        
        if "newkso.ru" in parsed_url.netloc:
            if self._iframe_context:
                iframe_origin = f"https://{urlparse(self._iframe_context).netloc}"
                newkso_headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
                    'Referer': self._iframe_context,
                    'Origin': iframe_origin
                }
                logger.info(f"Applied newkso.ru headers with iframe context for: {url}")
            else:
                newkso_origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
                newkso_headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
                    'Referer': newkso_origin,
                    'Origin': newkso_origin
                }
            headers.update(newkso_headers)
        
        return headers

    async def _handle_response_content(self, response: aiohttp.ClientResponse) -> str:
        """Gestisce la decompressione manuale del corpo della risposta (zstd, gzip, etc.)."""
        content_encoding = response.headers.get('Content-Encoding')
        raw_body = await response.read()
        
        try:
            if content_encoding == 'zstd':
                logger.info(f"Rilevata compressione zstd per {response.url}. Decompressione in corso...")
                try:
                    dctx = zstandard.ZstdDecompressor()
                    # ✅ MODIFICA: Utilizza stream_reader per gestire frame senza dimensione del contenuto.
                    # Questo risolve l'errore "could not determine content size in frame header".
                    with dctx.stream_reader(raw_body) as reader:
                        decompressed_body = reader.read()
                    return decompressed_body.decode(response.charset or 'utf-8')
                except zstandard.ZstdError as e:
                    logger.error(f"Errore di decompressione Zstd: {e}. Il contenuto potrebbe essere incompleto o corrotto.")
                    raise ExtractorError(f"Fallimento decompressione zstd: {e}")
            elif content_encoding == 'gzip':
                logger.info(f"Rilevata compressione gzip per {response.url}. Decompressione in corso...")
                decompressed_body = gzip.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            elif content_encoding == 'deflate':
                logger.info(f"Rilevata compressione deflate per {response.url}. Decompressione in corso...")
                decompressed_body = zlib.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            else:
                # Nessuna compressione o compressione non gestita, prova a decodificare direttamente
                # ✅ FIX: Usa 'errors=replace' per evitare crash su byte non validi
                return raw_body.decode(response.charset or 'utf-8', errors='replace')
        except Exception as e:
            logger.error(f"Errore durante la decompressione/decodifica del contenuto da {response.url}: {e}")
            raise ExtractorError(f"Fallimento decompressione per {response.url}: {e}")

    async def _make_robust_request(self, url: str, headers: dict = None, retries=3, initial_delay=2):
        """✅ Richieste con sessione persistente per evitare anti-bot"""
        final_headers = self._get_headers_for_url(url, headers or {})
        # Aggiungiamo zstd agli header accettati per segnalare al server che lo supportiamo
        # Rimosso 'br' perché non gestito in _handle_response_content
        final_headers['Accept-Encoding'] = 'gzip, deflate, zstd'
        
        for attempt in range(retries):
            try:
                # ✅ IMPORTANTE: Riusa sempre la stessa sessione
                session = await self._get_session()
                
                logger.info(f"Tentativo {attempt + 1}/{retries} per URL: {url}")
                async with session.get(url, headers=final_headers, ssl=False, auto_decompress=False) as response:
                    response.raise_for_status()
                    content = await self._handle_response_content(response)
                    
                    class MockResponse:
                        def __init__(self, text_content, status, headers_dict):
                            self._text = text_content
                            self.status = status
                            self.headers = headers_dict
                            self.url = url
                        
                        async def text(self):
                            return self._text
                            
                        def raise_for_status(self):
                            if self.status >= 400:
                                raise aiohttp.ClientResponseError(
                                    request_info=None, 
                                    history=None,
                                    status=self.status
                                )
                        
                        async def json(self):
                            return json.loads(self._text)
                    
                    logger.info(f"✅ Richiesta riuscita per {url} al tentativo {attempt + 1}")
                    return MockResponse(content, response.status, response.headers)
                    
            except (
                aiohttp.ClientConnectionError, 
                aiohttp.ServerDisconnectedError,
                aiohttp.ClientPayloadError,
                asyncio.TimeoutError,
                OSError,
                ConnectionResetError,
            ) as e:
                logger.warning(f"⚠️ Errore connessione tentativo {attempt + 1} per {url}: {str(e)}")
                
                # ✅ Solo in caso di errore critico, chiudi la sessione
                if attempt == retries - 1:
                    if self.session and not self.session.closed:
                        try:
                            await self.session.close()
                        except:
                            pass
                    self.session = None
                
                if attempt < retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    logger.info(f"⏳ Aspetto {delay} secondi prima del prossimo tentativo...")
                    await asyncio.sleep(delay)
                else:
                    raise ExtractorError(f"Tutti i {retries} tentativi falliti per {url}: {str(e)}")
                    
            except Exception as e:
                # Controlla se l'errore è dovuto a zstd e logga un messaggio specifico
                if 'zstd' in str(e).lower():
                    logger.critical(f"❌ Errore critico con la decompressione zstd. Assicurati che la libreria 'zstandard' sia installata (`pip install zstandard`). Errore: {e}")
                else: # type: ignore
                    logger.error(f"❌ Errore non di rete tentativo {attempt + 1} per {url}: {str(e)}")
                if attempt == retries - 1:
                    raise ExtractorError(f"Errore finale per {url}: {str(e)}")
        await asyncio.sleep(initial_delay)

    async def extract(self, url: str, force_refresh: bool = False, **kwargs) -> Dict[str, Any]:
        """Flusso di estrazione principale: risolve il dominio base, trova i player, estrae l'iframe, i parametri di autenticazione e l'URL m3u8 finale."""
        async def resolve_base_url(preferred_host: Optional[str] = None) -> str:
            """Risolve l'URL di base attivo provando una lista di domini noti."""
            if self._cached_base_url and not force_refresh:
                return self._cached_base_url
            
            DOMAINS = ['https://daddylive.sx/', 'https://daddyhd.com/']
            for base in DOMAINS:
                try:
                    resp = await self._make_robust_request(base, retries=1)
                    final_url = str(resp.url)
                    if not final_url.endswith('/'): final_url += '/' # Assicura lo slash finale
                    self._cached_base_url = final_url
                    logger.info(f"✅ Dominio base risolto: {final_url}")
                    return final_url
                except Exception as e:
                    logger.warning(f"⚠️ Tentativo fallito per il dominio base {base}: {e}")
            
            fallback = DOMAINS[0]
            logger.warning(f"Tutti i tentativi di risoluzione del dominio sono falliti, uso il fallback: {fallback}")
            self._cached_base_url = fallback
            return fallback

        def extract_channel_id(u: str) -> Optional[str]:
            patterns = [
                r'/premium(\d+)/mono\.m3u8$',
                r'/(?:watch|stream|cast|player)/stream-(\d+)\.php',
                r'watch\.php\?id=(\d+)',
                r'(?:%2F|/)stream-(\d+)\.php',
                r'stream-(\d+)\.php'
            ]
            for pattern in patterns:
                match = re.search(pattern, u, re.IGNORECASE)
                if match:
                    return match.group(1)
            return None

        async def get_stream_data(baseurl: str, initial_url: str, channel_id: str):
            daddy_origin = urlparse(baseurl).scheme + "://" + urlparse(baseurl).netloc
            daddylive_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
                'Referer': baseurl,
                'Origin': daddy_origin
            }
            
            # 1. Richiesta pagina iniziale per trovare i link dei player
            resp1 = await self._make_robust_request(initial_url, headers=daddylive_headers)
            content1 = await resp1.text()
            player_links = re.findall(r'<button[^>]*data-url="([^"]+)"[^>]*>Player\s*\d+</button>', content1)
            if not player_links:
                raise ExtractorError("Nessun link player trovato nella pagina.")
            
            last_player_error = None
            iframe_candidates = []
            
            for player_url in player_links:
                try:
                    if not player_url.startswith('http'):
                        player_url = urljoin(baseurl, player_url)
            
                    daddylive_headers['Referer'] = player_url
                    resp2 = await self._make_robust_request(player_url, headers=daddylive_headers)
                    content2 = await resp2.text()
                    iframes2 = re.findall(r'<iframe.*?src="([^"]*)"', content2)
            
                    for iframe in iframes2:
                        full_iframe_url = urljoin(player_url, iframe)
                        if full_iframe_url not in iframe_candidates:
                            iframe_candidates.append(full_iframe_url)
                            logger.info(f"Found iframe candidate: {full_iframe_url}")
            
                except Exception as e:
                    last_player_error = e
                    logger.warning(f"Fallito il processamento del link player {player_url}: {e}")
                    continue
            
            if not iframe_candidates:
                if last_player_error:
                    raise ExtractorError(f"Tutti i link dei player sono falliti. Ultimo errore: {last_player_error}")
                raise ExtractorError("Nessun iframe valido trovato in nessuna pagina player")
            
            last_iframe_error = None
            for iframe_candidate in iframe_candidates:
                try:
                    logger.info(f"Trying iframe: {iframe_candidate}")
                    iframe_domain = urlparse(iframe_candidate).netloc
                    if not iframe_domain:
                        logger.warning(f"Invalid iframe URL format: {iframe_candidate}")
                        continue
            
                    self._iframe_context = iframe_candidate
                    resp3 = await self._make_robust_request(iframe_candidate, headers=daddylive_headers)
                    iframe_content = await resp3.text()
                    logger.info(f"Successfully loaded iframe from: {iframe_domain}")
            
                    if 'lovecdn.ru' in iframe_domain:
                        logger.info("Detected lovecdn.ru iframe - using alternative extraction")
                        result = await self._extract_lovecdn_stream(iframe_candidate, iframe_content, daddylive_headers)
                        self._stream_data_cache[channel_id] = result
                        self._save_cache()
                        return result
                    else:
                        logger.info("Attempting new auth flow extraction.")
                        result = await self._extract_new_auth_flow(iframe_candidate, iframe_content, daddylive_headers)
                        self._stream_data_cache[channel_id] = result
                        self._save_cache()
                        return result
            
                except Exception as e:
                    logger.warning(f"Failed to process iframe {iframe_candidate}: {e}")
                    last_iframe_error = e
                    continue
            
            raise ExtractorError(f"All iframe candidates failed. Last error: {last_iframe_error}")

        try:
            channel_id = extract_channel_id(url)
            if not channel_id:
                raise ExtractorError(f"Impossibile estrarre channel ID da {url}")

            # Controlla la cache prima di procedere
            if not force_refresh and channel_id in self._stream_data_cache:
                logger.info(f"✅ Trovati dati in cache per il canale ID: {channel_id}. Verifico la validità...")
                cached_data = self._stream_data_cache[channel_id]
                stream_url = cached_data.get("destination_url")
                stream_headers = cached_data.get("request_headers", {})

                is_valid = False
                if stream_url:
                    try:
                        # Usa una sessione separata per la validazione per non interferire
                        # con la sessione principale e i suoi cookie.
                        async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as validation_session:
                            async with validation_session.head(stream_url, headers=stream_headers, ssl=False) as response:
                                # Uso una richiesta HEAD per efficienza, con un timeout breve
                                if response.status == 200:
                                    is_valid = True
                                    logger.info(f"✅ Cache per il canale ID {channel_id} è valida.")
                                else:
                                    logger.warning(f"⚠️ Cache per il canale ID {channel_id} non valida. Status: {response.status}. Procedo con estrazione.")
                    except Exception as e:
                        logger.warning(f"⚠️ Errore durante la validazione della cache per {channel_id}: {e}. Procedo con estrazione.")
                
                if not is_valid:
                    # Rimuovi i dati invalidi dalla cache
                    if channel_id in self._stream_data_cache:
                        del self._stream_data_cache[channel_id]
                    self._save_cache()
                    logger.info(f"🗑️ Cache invalidata per il canale ID {channel_id}.")
                else:
                    # ✅ NUOVO: Esegui una richiesta di "keep-alive" per mantenere la sessione attiva
                    # Questo utilizza il proxy se configurato, come richiesto.
                    try:
                        logger.info(f"🔄 Eseguo una richiesta di keep-alive per il canale {channel_id} per mantenere la sessione attiva tramite proxy.")
                        baseurl = await resolve_base_url()
                        # Eseguiamo una richiesta leggera alla pagina del canale per aggiornare i cookie di sessione.
                        # Questo assicura che il proxy venga utilizzato.
                        await self._make_robust_request(url, retries=1)
                        logger.info(f"✅ Sessione per il canale {channel_id} rinfrescata con successo.")
                    except Exception as e:
                        logger.warning(f"⚠️ Fallita la richiesta di keep-alive per il canale {channel_id}: {e}. Lo stream potrebbe non funzionare.")
                    
                    return cached_data

            # ✅ NUOVO: Usa un lock per prevenire estrazioni simultanee per lo stesso canale
            if channel_id not in self._extraction_locks:
                self._extraction_locks[channel_id] = asyncio.Lock()
            
            lock = self._extraction_locks[channel_id]
            async with lock:
                # Ricontrolla la cache dopo aver acquisito il lock, un altro processo potrebbe averla già popolata
                if channel_id in self._stream_data_cache:
                    logger.info(f"✅ Dati per il canale {channel_id} trovati in cache dopo aver atteso il lock.")
                    return self._stream_data_cache[channel_id]

                # Procedi con l'estrazione
                logger.info(f"⚙️ Nessuna cache valida per {channel_id}, avvio estrazione completa...")
                baseurl = await resolve_base_url()
                return await get_stream_data(baseurl, url, channel_id)
            
        except Exception as e:
            # Per errori 403, non loggare il traceback perché sono errori attesi (servizio temporaneamente non disponibile)
            error_message = str(e)
            if "403" in error_message or "Forbidden" in error_message:
                logger.error(f"Estrazione DLHD completamente fallita per URL {url}")
            else:
                logger.exception(f"Estrazione DLHD completamente fallita per URL {url}")
            raise ExtractorError(f"Estrazione DLHD completamente fallita: {str(e)}")

    async def _extract_lovecdn_stream(self, iframe_url: str, iframe_content: str, headers: dict) -> Dict[str, Any]:
        """
        Estrattore alternativo per iframe lovecdn.ru che usa un formato diverso.
        """
        try:
            # Cerca pattern di stream URL diretto
            m3u8_patterns = [
                r'["\']([^"\']*\.m3u8[^"\']*)["\']',
                r'source[:\s]+["\']([^"\']+)["\']',
                r'file[:\s]+["\']([^"\']+\.m3u8[^"\']*)["\']',
                r'hlsManifestUrl[:\s]*["\']([^"\']+)["\']',
            ]
            
            stream_url = None
            for pattern in m3u8_patterns:
                matches = re.findall(pattern, iframe_content)
                for match in matches:
                    if '.m3u8' in match and match.startswith('http'):
                        stream_url = match
                        logger.info(f"Found direct m3u8 URL: {stream_url}")
                        break
                if stream_url:
                    break
            
            # Pattern 2: Cerca costruzione dinamica URL
            if not stream_url:
                channel_match = re.search(r'(?:stream|channel)["\s:=]+["\']([^"\']+)["\']', iframe_content)
                server_match = re.search(r'(?:server|domain|host)["\s:=]+["\']([^"\']+)["\']', iframe_content)
                
                if channel_match:
                    channel_name = channel_match.group(1)
                    server = server_match.group(1) if server_match else 'newkso.ru'
                    stream_url = f"https://{server}/{channel_name}/mono.m3u8"
                    logger.info(f"Constructed stream URL: {stream_url}")
            
            if not stream_url:
                # Fallback: cerca qualsiasi URL che sembri uno stream
                url_pattern = r'https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*'
                matches = re.findall(url_pattern, iframe_content)
                if matches:
                    stream_url = matches[0]
                    logger.info(f"Found fallback stream URL: {stream_url}")
            
            if not stream_url:
                raise ExtractorError(f"Could not find stream URL in lovecdn.ru iframe")
            
            # Usa iframe URL come referer
            iframe_origin = f"https://{urlparse(iframe_url).netloc}"
            stream_headers = {
                'User-Agent': headers['User-Agent'],
                'Referer': iframe_url,
                'Origin': iframe_origin
            }
            
            # Determina endpoint in base al dominio dello stream
            # Se è un m3u8 standard, usa hls_manifest_proxy, se richiede chiavi speciali, potrebbe servire altro
            # Ma per ora manteniamo hls_manifest_proxy come nel codice originale
            
            logger.info(f"Using lovecdn.ru stream extraction")
            
            return {
                "destination_url": stream_url,
                "request_headers": stream_headers,
                "mediaflow_endpoint": self.mediaflow_endpoint,
            }
            
        except Exception as e:
            raise ExtractorError(f"Failed to extract lovecdn.ru stream: {e}")

    async def _extract_new_auth_flow(self, iframe_url: str, iframe_content: str, headers: dict) -> Dict[str, Any]:
        """Gestisce il nuovo flusso di autenticazione."""
        
        def _extract_params(js: str) -> Dict[str, Optional[str]]:
            params = {}
            patterns = {
                "channel_key": r'(?:const|var|let)\s+(?:CHANNEL_KEY|channelKey)\s*=\s*["\']([^"\']+)["\']',
                "auth_token": r'(?:const|var|let)\s+AUTH_TOKEN\s*=\s*["\']([^"\']+)["\']',
                "auth_country": r'(?:const|var|let)\s+AUTH_COUNTRY\s*=\s*["\']([^"\']+)["\']',
                "auth_ts": r'(?:const|var|let)\s+AUTH_TS\s*=\s*["\']([^"\']+)["\']',
                "auth_expiry": r'(?:const|var|let)\s+AUTH_EXPIRY\s*=\s*["\']([^"\']+)["\']',
            }
            for key, pattern in patterns.items():
                match = re.search(pattern, js)
                params[key] = match.group(1) if match else None
            return params

        params = _extract_params(iframe_content)
        
        missing_params = [k for k, v in params.items() if not v]
        if missing_params:
            # This is not an error, just means it's not the new flow
            raise ExtractorError(f"Not the new auth flow: missing params {missing_params}")

        logger.info("New auth flow detected. Proceeding with POST auth.")
        
        # 1. Initial Auth POST
        auth_url = 'https://security.newkso.ru/auth2.php'
        form_data = FormData()
        form_data.add_field('channelKey', params["channel_key"])
        form_data.add_field('country', params["auth_country"])
        form_data.add_field('timestamp', params["auth_ts"])
        form_data.add_field('expiry', params["auth_expiry"])
        form_data.add_field('token', params["auth_token"])

        iframe_origin = f"https://{urlparse(iframe_url).netloc}"
        auth_headers = headers.copy()
        auth_headers.update({
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': iframe_origin,
            'Referer': iframe_url,
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'Priority': 'u=1, i',
        })
        
        try:
            session = await self._get_session()
            async with session.post(auth_url, data=form_data, headers=auth_headers, ssl=False) as auth_resp:
                auth_resp.raise_for_status()
                auth_data = await auth_resp.json()
                if not (auth_data.get("valid") or auth_data.get("success")):
                    raise ExtractorError(f"Initial auth failed with response: {auth_data}")
            logger.info("New auth flow: Initial auth successful.")
        except Exception as e:
            raise ExtractorError(f"New auth flow failed during initial auth POST: {e}")

        # 2. Server Lookup
        server_lookup_url = f"https://{urlparse(iframe_url).netloc}/server_lookup.js?channel_id={params['channel_key']}"
        try:
            lookup_resp = await self._make_robust_request(server_lookup_url, headers=headers)
            server_data = await lookup_resp.json()
            server_key = server_data.get('server_key')
            if not server_key:
                raise ExtractorError(f"No server_key in lookup response: {server_data}")
            logger.info(f"New auth flow: Server lookup successful - Server key: {server_key}")
        except Exception as e:
            raise ExtractorError(f"New auth flow failed during server lookup: {e}")

        # 3. Build final stream URL
        channel_key = params['channel_key']
        auth_token = params['auth_token']
        # The JS logic uses .css, not .m3u8
        if server_key == 'top1/cdn':
            stream_url = f'https://top1.newkso.ru/top1/cdn/{channel_key}/mono.css'
        else:
            stream_url = f'https://{server_key}new.newkso.ru/{server_key}/{channel_key}/mono.css'
        
        logger.info(f'New auth flow: Constructed stream URL: {stream_url}')

        stream_headers = {
            'User-Agent': headers['User-Agent'],
            'Referer': iframe_url,
            'Origin': iframe_origin,
            'Authorization': f'Bearer {auth_token}',
            'X-Channel-Key': channel_key
        }

        return {
            "destination_url": stream_url,
            "request_headers": stream_headers,
            "mediaflow_endpoint": self.mediaflow_endpoint,
        }

    async def invalidate_cache_for_url(self, url: str):
        """
        Invalida la cache per un URL specifico.
        Questa funzione viene chiamata da app.py quando rileva un errore (es. fallimento chiave AES).
        """
        def extract_channel_id_internal(u: str) -> Optional[str]:
            patterns = [
                r'/premium(\d+)/mono\.m3u8$',
                r'/(?:watch|stream|cast|player)/stream-(\d+)\.php',
                r'watch\.php\?id=(\d+)',
                r'(?:%2F|/)stream-(\d+)\.php',
                r'stream-(\d+)\.php'
            ]
            for pattern in patterns:
                match = re.search(pattern, u, re.IGNORECASE)
                if match: return match.group(1)
            return None

        channel_id = extract_channel_id_internal(url)
        if channel_id and channel_id in self._stream_data_cache:
            del self._stream_data_cache[channel_id]
            self._save_cache()
            logger.info(f"🗑️ Cache per il canale ID {channel_id} invalidata a causa di un errore esterno (es. chiave AES).")

    async def close(self):
        """Chiude definitivamente la sessione"""
        if self.session and not self.session.closed:
            try:
                await self.session.close()
            except:
                pass
        self.session = None
