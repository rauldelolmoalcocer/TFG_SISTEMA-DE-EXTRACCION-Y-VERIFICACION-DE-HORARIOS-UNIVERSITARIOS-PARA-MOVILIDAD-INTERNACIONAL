import os
import re
import time
import queue
import threading
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse, urldefrag, unquote

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor


DEFAULT_USER_AGENT = "TFG-PDF-Crawler/1.0 (+academic-project)"


@dataclass
class CrawlerConfig:
    start_url: str
    download_folder: str
    max_depth: int = 0
    max_pages: int = 1
    max_download_workers: int = 4
    request_timeout: int = 15
    delay_between_requests: float = 0.5
    same_domain_only: bool = True
    verify_ssl: bool = True
    overwrite_files: bool = False


@dataclass
class CrawlerState:
    running: bool = False

    visited_pages: set = field(default_factory=set)
    discovered_pdfs: set = field(default_factory=set)
    downloaded_urls: set = field(default_factory=set)

    saved_filepaths: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    logs: list = field(default_factory=list)

    total_pages_crawled: int = 0
    total_pdfs_found: int = 0
    total_pdfs_downloaded: int = 0

    lock: threading.Lock = field(default_factory=threading.Lock)

    def add_log(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        line = f"[{timestamp}] {message}"
        with self.lock:
            self.logs.append(line)

    def add_error(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        line = f"[{timestamp}] ERROR: {message}"
        with self.lock:
            self.errors.append(line)
            self.logs.append(line)


class PdfCrawler:
    def __init__(self, config: CrawlerConfig, state: CrawlerState):
        self.config = config
        self.state = state

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": DEFAULT_USER_AGENT
        })

        self.base_domain = urlparse(config.start_url).netloc.lower()
        self.page_queue = queue.Queue()

        os.makedirs(self.config.download_folder, exist_ok=True)

    def run(self):
        self.state.running = True
        self.state.add_log(f"Inicio del crawler desde: {self.config.start_url}")
        self.page_queue.put((self.config.start_url, 0))

        try:
            with ThreadPoolExecutor(max_workers=self.config.max_download_workers) as executor:
                download_futures = []

                while not self.page_queue.empty():
                    limit_reached = False

                    with self.state.lock:
                        if self.state.total_pages_crawled >= self.config.max_pages:
                            limit_reached = True

                    if limit_reached:
                        self.state.add_log("Límite de páginas alcanzado.")
                        break

                    current_url, depth = self.page_queue.get()

                    normalized = self.normalize_url(current_url)
                    if not normalized:
                        continue

                    if depth > self.config.max_depth:
                        continue

                    with self.state.lock:
                        if normalized in self.state.visited_pages:
                            continue
                        self.state.visited_pages.add(normalized)
                        self.state.total_pages_crawled += 1

                    self.state.add_log(f"Rastreando página: {normalized} (profundidad={depth})")

                    try:
                        response = self.fetch_url(normalized)
                    except Exception as e:
                        self.state.add_error(f"No se pudo acceder a {normalized}: {e}")
                        continue

                    if response is None:
                        continue

                    content_type = response.headers.get("Content-Type", "").lower()
                    self.state.add_log(f"Content-Type recibido: {content_type or 'desconocido'}")

                    # Si la URL principal ya apunta directamente a un PDF
                    if self.is_pdf_url(normalized) or "application/pdf" in content_type:
                        is_new_pdf = False

                        with self.state.lock:
                            if normalized not in self.state.discovered_pdfs:
                                self.state.discovered_pdfs.add(normalized)
                                self.state.total_pdfs_found += 1
                                is_new_pdf = True

                        if is_new_pdf:
                            self.state.add_log(f"PDF detectado: {normalized}")

                        future = executor.submit(self.download_pdf, normalized, response)
                        download_futures.append(future)
                        continue

                    # Si no es HTML, no seguimos parseando
                    if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                        self.state.add_log(f"Saltando recurso no HTML: {normalized}")
                        continue

                    try:
                        links = self.extract_links(response.text, normalized)
                        self.state.add_log(f"Enlaces encontrados en la página principal: {len(links)}")
                    except Exception as e:
                        self.state.add_error(f"Error parseando HTML en {normalized}: {e}")
                        continue

                    # SOLO se buscan PDFs en la página principal.
                    # NO se meten otros HTML en la cola.
                    for link in links:
                        if self.should_skip_url(link):
                            continue

                        if self.is_pdf_url(link):
                            is_new_pdf = False

                            with self.state.lock:
                                if link not in self.state.discovered_pdfs:
                                    self.state.discovered_pdfs.add(link)
                                    self.state.total_pdfs_found += 1
                                    is_new_pdf = True

                            if is_new_pdf:
                                self.state.add_log(f"PDF detectado: {link}")
                                future = executor.submit(self.download_pdf, link, None)
                                download_futures.append(future)

                    time.sleep(self.config.delay_between_requests)

                for future in download_futures:
                    try:
                        future.result()
                    except Exception as e:
                        self.state.add_error(f"Error en descarga paralela: {e}")

        finally:
            self.state.running = False
            self.state.add_log(
                f"Crawler finalizado. Páginas rastreadas: {self.state.total_pages_crawled}, "
                f"PDFs encontrados: {self.state.total_pdfs_found}, "
                f"PDFs descargados: {self.state.total_pdfs_downloaded}"
            )

    def fetch_url(self, url: str):
        response = self.session.get(
            url,
            timeout=self.config.request_timeout,
            allow_redirects=True,
            verify=self.config.verify_ssl
        )
        response.raise_for_status()
        return response

    def extract_links(self, html: str, base_url: str):
        soup = BeautifulSoup(html, "html.parser")
        links = set()

        for tag in soup.find_all("a", href=True):
            href = tag.get("href", "").strip()
            if not href:
                continue

            absolute_url = urljoin(base_url, href)
            absolute_url = self.normalize_url(absolute_url)

            if absolute_url:
                links.add(absolute_url)

        return links

    def normalize_url(self, url: str):
        if not url:
            return None

        url = url.strip()
        url, _ = urldefrag(url)

        parsed = urlparse(url)

        if parsed.scheme not in ("http", "https"):
            return None

        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        path = parsed.path or "/"

        return parsed._replace(
            scheme=scheme,
            netloc=netloc,
            path=path,
            fragment=""
        ).geturl()

    def should_skip_url(self, url: str):
        parsed = urlparse(url)

        if parsed.scheme not in ("http", "https"):
            return True

        if self.config.same_domain_only and parsed.netloc.lower() != self.base_domain:
            return True

        lower_url = url.lower()

        skip_patterns = (
            ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
            ".css", ".js", ".zip", ".rar", ".7z",
            ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"
        )

        return any(lower_url.endswith(p) for p in skip_patterns)

    def is_pdf_url(self, url: str):
        parsed = urlparse(url)
        return parsed.path.lower().endswith(".pdf")

    def build_safe_filename(self, url: str):
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path)

        if not filename or not filename.lower().endswith(".pdf"):
            raw_name = f"{parsed.netloc}{parsed.path}"
            filename = re.sub(r"[^a-zA-Z0-9._-]", "_", raw_name).strip("_") + ".pdf"

        filename = unquote(filename)
        filename = re.sub(r"[^\w.\-]", "_", filename)

        if not filename.lower().endswith(".pdf"):
            filename += ".pdf"

        return filename

    def ensure_unique_filepath(self, filepath: str):
        if self.config.overwrite_files or not os.path.exists(filepath):
            return filepath

        base, ext = os.path.splitext(filepath)
        counter = 1

        while True:
            candidate = f"{base}_{counter}{ext}"
            if not os.path.exists(candidate):
                return candidate
            counter += 1

    def download_pdf(self, pdf_url: str, existing_response=None):
        with self.state.lock:
            if pdf_url in self.state.downloaded_urls:
                return
            self.state.downloaded_urls.add(pdf_url)

        self.state.add_log(f"Descargando PDF: {pdf_url}")

        try:
            response = existing_response

            if response is None:
                response = self.session.get(
                    pdf_url,
                    timeout=self.config.request_timeout,
                    allow_redirects=True,
                    stream=True,
                    verify=self.config.verify_ssl
                )
                response.raise_for_status()

            content_type = response.headers.get("Content-Type", "").lower()

            if "application/pdf" not in content_type and not self.is_pdf_url(pdf_url):
                self.state.add_log(f"Descartado, no parece PDF real: {pdf_url}")
                with self.state.lock:
                    self.state.downloaded_urls.discard(pdf_url)
                return

            filename = self.build_safe_filename(pdf_url)
            filepath = os.path.join(self.config.download_folder, filename)
            filepath = self.ensure_unique_filepath(filepath)

            with open(filepath, "wb") as f:
                if existing_response is not None:
                    f.write(response.content)
                else:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

            with self.state.lock:
                self.state.saved_filepaths.append(filepath)
                self.state.total_pdfs_downloaded += 1

            self.state.add_log(f"PDF guardado en: {filepath}")

        except Exception as e:
            with self.state.lock:
                self.state.downloaded_urls.discard(pdf_url)
            self.state.add_error(f"Fallo descargando {pdf_url}: {e}")