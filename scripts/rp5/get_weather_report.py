"""
Module for downloading and processing weather archive data from rp5.ru
with proxy support, headless mode, and detailed logging.
"""
import os
import re
import sys
import time
import gzip
import socket
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Set
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from selenium.common.exceptions import TimeoutException, WebDriverException

def _parse_proxy_for_chrome(proxy: str) -> Tuple[str, str]:
    """
    Парсит прокси-строку для Chrome.
    Возвращает (proxy_for_chrome, proxy_type).
    Поддерживает: http, https, socks5, socks5h, socks4.
    """
    proxy = proxy.strip()
    
    # Протоколы, которые Chrome понимает
    known_prefixes = ('socks5://', 'socks5h://', 'socks4://', 'http://', 'https://')
    
    if any(proxy.startswith(p) for p in known_prefixes):
        # Уже с префиксом — извлекаем тип
        for prefix in known_prefixes:
            if proxy.startswith(prefix):
                proxy_type = prefix[:-3]  # 'socks5://' -> 'socks5'
                return proxy, proxy_type
        return proxy, 'http'
    
    # Нет префикса — определяем по порту или считаем HTTP
    server_part = proxy.split('@')[-1] if '@' in proxy else proxy
    
    if ':' in server_part:
        _, port = server_part.rsplit(':', 1)
        # Типичные порты SOCKS
        if port in ('1080', '1085', '9999', '1088', '4145', '4153'):
            return f"socks5://{proxy}", 'socks5'
    
    # По умолчанию HTTP
    return proxy, 'http'

# ============================================================================
# НАСТРОЙКА ДЕТАЛЬНОГО ЛОГИРОВАНИЯ
# ============================================================================
def setup_detailed_logging(log_file: str = 'rp5_downloader.log', level: int = logging.DEBUG):
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    
    file_formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)s:%(lineno)d | '
        '%(funcName)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    logging.info(f"🚀 [DOWNLOADER] Логирование инициализировано | Файл: {os.path.abspath(log_file)}")

setup_detailed_logging()
logger = logging.getLogger(__name__)


# ============================================================================
# КЛАСС ДЛЯ УПРАВЛЕНИЯ ПРОКСИ С ЧЁРНЫМ СПИСКОМ
# ============================================================================
class ProxyManager:
    """Менеджер прокси с тестированием, ротацией и чёрным списком"""
    
    DEFAULT_PROXY_FILE = "./proxies.txt"
    TEST_TIMEOUT = 0.5
    
    def __init__(self, proxy_file: Optional[str] = None, test_timeout: float = None):
        self.proxy_file = proxy_file or self.DEFAULT_PROXY_FILE
        self.test_timeout = test_timeout or self.TEST_TIMEOUT
        self.proxies: List[str] = self._load_proxies()
        self.bad_proxies: Set[str] = set()  # Чёрный список — прокси больше не используются
        self.current_index: int = 0
        self.active_proxy: Optional[str] = None
        
        logger.debug(f"[ProxyManager] Инициализирован | Файл: {self.proxy_file} | Прокси: {len(self.proxies)} | Забанено: 0")
    
    def _load_proxies(self) -> List[str]:
        if not os.path.exists(self.proxy_file):
            logger.warning(f"[ProxyManager] Файл не найден: {self.proxy_file}")
            return []
        
        try:
            with open(self.proxy_file, 'r', encoding='utf-8') as f:
                proxies = [
                    line.strip() 
                    for line in f 
                    if line.strip() and not line.startswith('#') and ':' in line
                ]
            logger.info(f"[ProxyManager] ✓ Загружено {len(proxies)} прокси")
            return proxies
        except Exception as e:
            logger.error(f"[ProxyManager] ❌ Ошибка загрузки: {e}", exc_info=True)
            return []
    
    def _test_proxy_connection(self, proxy: str, timeout: float = None) -> bool:
        if timeout is None:
            timeout = self.test_timeout
        
        logger.debug(f"[ProxyManager] Тест прокси: {proxy}")
        
        try:
            # Парсим прокси для определения типа
            proxy_formatted, proxy_type = _parse_proxy_for_chrome(proxy)
            
            # Извлекаем host:port (без auth и префиксов)
            server_part = proxy_formatted
            for prefix in ('socks5://', 'socks5h://', 'socks4://', 'http://', 'https://'):
                if server_part.startswith(prefix):
                    server_part = server_part[len(prefix):]
                    break
            
            # Убираем авторизацию если есть
            if '@' in server_part:
                server_part = server_part.split('@')[-1]
            
            if ':' not in server_part:
                return False
            host, port = server_part.rsplit(':', 1)
            port = int(port)
            
            # Для SOCKS — простая проверка доступности порта
            # Полноценный SOCKS-хендшейк сложен, полагаемся на Chrome
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            try:
                result = sock.connect_ex((host, port))
                # Для SOCKS считаем успешным просто доступность порта
                return result == 0
            finally:
                sock.close()
                
        except Exception as e:
            logger.debug(f"[ProxyManager] ✗ Ошибка теста {proxy}: {e}")
            return False
    
    def mark_proxy_bad(self, proxy: str) -> None:
        """Добавляет прокси в чёрный список — он больше не будет использоваться"""
        if proxy and proxy not in self.bad_proxies:
            self.bad_proxies.add(proxy)
            logger.debug(f"[ProxyManager] 🚫 Прокси добавлен в чёрный список: {proxy} (всего забанено: {len(self.bad_proxies)})")
    
    def is_proxy_bad(self, proxy: str) -> bool:
        """Проверяет, находится ли прокси в чёрном списке"""
        return proxy in self.bad_proxies
    
    def get_working_proxy(self) -> Optional[str]:
        """Перебирает ВСЕ прокси, пропуская забаненные, пока не найдёт рабочий"""
        if not self.proxies:
            return None
        
        available = [p for p in self.proxies if p not in self.bad_proxies]
        if not available:
            logger.warning(f"[ProxyManager] ⚠ Все {len(self.proxies)} прокси в чёрном списке")
            return None
        
        logger.info(f"[ProxyManager] 🔍 Поиск рабочего прокси (доступно: {len(available)}/{len(self.proxies)})...")
        
        start_idx = self.current_index
        tested = 0
        
        # Перебираем все прокси начиная с текущего индекса
        for _ in range(len(self.proxies)):
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
            
            # Пропускаем забаненные
            if proxy in self.bad_proxies:
                logger.debug(f"  [SKIP] Забанен: {proxy}")
                continue
            
            tested += 1
            logger.debug(f"  [{tested}/{len(available)}] Тест: {proxy}")
            
            if self._test_proxy_connection(proxy, self.test_timeout):
                logger.info(f"[ProxyManager] ✓ Рабочий прокси: {proxy}")
                self.active_proxy = proxy
                return proxy
            
            logger.debug(f"  ✗ Не прошёл тест: {proxy}")
            time.sleep(0.03)
        
        logger.warning(f"[ProxyManager] ⚠ Протестировано {tested} доступных прокси — ни один не ответил")
        return None
    
    def get_proxy_for_chrome(self) -> Optional[str]:
        proxy = self.active_proxy or self.get_working_proxy()
        if not proxy:
            return None
        
        if '@' in proxy:
            logger.warning(f"[ProxyManager] ⚠ Прокси с авторизацией может требовать расширения: {proxy}")
            return proxy.split('@')[-1]
        return proxy
    
    def get_stats(self) -> dict:
        """Возвращает статистику по прокси"""
        return {
            'total': len(self.proxies),
            'bad': len(self.bad_proxies),
            'available': len(self.proxies) - len(self.bad_proxies),
            'active': self.active_proxy
        }


# ============================================================================
# ОСНОВНЫЕ ФУНКЦИИ МОДУЛЯ
# ============================================================================

def extract_city_name(url: str) -> str:
    match = re.search(r'_in_([^/]+?)(?:_\(|$)', url)
    if match:
        return match.group(1).strip()
    return "unknown"


def _wait_for_download_complete(download_dir: str, timeout: int = 120) -> Optional[str]:
    logger.debug(f"[_wait_for_download_complete] Мониторинг: {download_dir}, таймаут: {timeout}с")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        gz_files = [
            f for f in os.listdir(download_dir) 
            if f.endswith('.gz') and not f.endswith('.crdownload')
        ]
        
        if gz_files:
            file_path = os.path.join(download_dir, gz_files[0])
            prev_size = -1
            stable_count = 0
            
            for check in range(15):
                time.sleep(1)
                try:
                    curr_size = os.path.getsize(file_path)
                    logger.debug(f"  [Проверка {check+1}] Размер: {curr_size} байт")
                    
                    if curr_size == prev_size and curr_size > 100:
                        stable_count += 1
                        if stable_count >= 3:
                            logger.info(f"✓ Загрузка завершена: {gz_files[0]} ({curr_size} байт)")
                            return file_path
                    prev_size = curr_size
                except FileNotFoundError:
                    logger.debug("  ✗ Файл исчез, перезапуск проверки")
                    break
        
        crdownload_files = [f for f in os.listdir(download_dir) if f.endswith('.crdownload')]
        if crdownload_files:
            logger.debug(f"  ⏳ Активные загрузки: {crdownload_files}")
        
        time.sleep(1)
    
    logger.warning(f"⏱ Таймаут ожидания загрузки ({timeout}с) в {download_dir}")
    return None


def _safe_get(url: str, driver, timeout: int = 60, proxy_manager: Optional[ProxyManager] = None) -> bool:
    url = url.strip()
    
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            logger.debug(f"[_safe_get] Попытка {attempt+1}/{max_retries}: {url[:100]}... (прокси: {proxy_manager.active_proxy if proxy_manager else 'нет'})")
            
            # driver.set_page_load_timeout(timeout if attempt > 0 else 10)
            driver.get(url)
            
            time.sleep(3)
            
            current_url = driver.current_url
            logger.debug(f"  → Текущий URL: {current_url[:100]}...")
            
            if current_url.startswith(('data:')):
                logger.warning(f"  ⚠ Служебная страница: {current_url}")
                # Помечаем прокси как плохой
                if proxy_manager and proxy_manager.active_proxy:
                    proxy_manager.mark_proxy_bad(proxy_manager.active_proxy)
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return False
            
            if "error 404" in driver.page_source.lower() or "страница не найдена" in driver.page_source.lower():
                logger.debug("  ✗ Обнаружен 404")
                return False
            
            logger.info(f"✓ Страница загружена успешно")
            return True
            
        except TimeoutException as e:
            logger.warning(f"⏱ Таймаут загрузки (попытка {attempt+1}): {e}")
            
            # Помечаем прокси как плохой при таймауте
            if proxy_manager and proxy_manager.active_proxy:
                proxy_manager.mark_proxy_bad(proxy_manager.active_proxy)
                logger.warning(f"🚫 Прокси забанен из-за таймаута: {proxy_manager.active_proxy}")
            
            if attempt == 0 and proxy_manager and proxy_manager.proxies:
                logger.debug("→ Пробуем сменить прокси...")
                if proxy_manager.get_working_proxy():
                    raise ProxyRotationNeeded(f"Proxy rotation requested after timeout")
            
            if attempt == max_retries - 1:
                return False
            time.sleep(2 ** attempt)
            
        except WebDriverException as e:
            error_str = str(e).lower()
            logger.error(f"❌ WebDriverException: {type(e).__name__}: {str(e)[:200]}")
            
            # Помечаем прокси как плохой при ошибках подключения
            if proxy_manager and proxy_manager.active_proxy:
                proxy_errors = ["proxy", "tunnel", "connection failed", "err_", "net::", "unable to connect"]
                if any(err in error_str for err in proxy_errors):
                    logger.warning("→ Ошибка связана с прокси, баним и пробуем ротацию")
                    proxy_manager.mark_proxy_bad(proxy_manager.active_proxy)
                    if proxy_manager.get_working_proxy():
                        raise ProxyRotationNeeded(f"Proxy rotation requested after WebDriver error")
            
            if attempt == max_retries - 1:
                return False
            time.sleep(2 ** attempt)
            
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка: {type(e).__name__}: {e}", exc_info=True)
            
            # Помечаем прокси при любых ошибках
            if proxy_manager and proxy_manager.active_proxy:
                proxy_manager.mark_proxy_bad(proxy_manager.active_proxy)
                logger.debug(f"🚫 Прокси забанен из-за ошибки: {type(e).__name__}")
            
            if attempt == max_retries - 1:
                return False
            time.sleep(2 ** attempt)
    
    return False


class ProxyRotationNeeded(Exception):
    pass


def _create_driver_with_proxy(
    download_dir: str, 
    proxy_manager: Optional[ProxyManager] = None,
    headless: bool = True
) -> webdriver.Chrome:
    logger.debug(f"[_create_driver_with_proxy] headless={headless}, proxy={proxy_manager.active_proxy if proxy_manager else None}")
    
    options = webdriver.ChromeOptions()
    
    chrome_paths = ["/usr/bin/chromium", "/usr/bin/chromium-browser", "/usr/bin/google-chrome"]
    for path in chrome_paths:
        if os.path.exists(path):
            options.binary_location = path
            logger.debug(f"  ✓ Chrome binary: {path}")
            break
    
    if headless:
        options.add_argument("--headless=new")
        logger.debug("  ✓ Включён headless режим")
    
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-certificate-errors-spki-list")
    options.add_argument("--ignore-ssl-errors")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--disable-web-security")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_setting_values": {
            "automatic_downloads": 1,
            "images": 2,
            "notifications": 2,
        },
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False
    }
    options.add_experimental_option("prefs", prefs)
    logger.debug("  ✓ Настройки загрузок применены")
    
    if proxy_manager:
        proxy_raw = proxy_manager.get_proxy_for_chrome()
        if proxy_raw:
            proxy_formatted, proxy_type = _parse_proxy_for_chrome(proxy_raw)
            options.add_argument(f"--proxy-server={proxy_formatted}")
            options.add_argument("--proxy-bypass-list=<-loopback>")
            logger.info(f"  ✓ Прокси настроен: {proxy_formatted} (тип: {proxy_type})")
        else:
            logger.debug("  → Работа без прокси (не найден рабочий)")
    else:
        logger.debug("  → ProxyManager не передан, работа без прокси")
    
    logger.info("  🔄 Инициализация WebDriver...")
    start = time.time()
    
    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()),
            options=options
        )
        
        driver.implicitly_wait(20)
        driver.set_page_load_timeout(120)
        
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                window.chrome = {runtime: {}};
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
                Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
            """
        })
        
        elapsed = time.time() - start
        logger.info(f"  ✓ WebDriver запущен за {elapsed:.2f}с")
        return driver
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска WebDriver: {e}", exc_info=True)
        raise


def download_and_process(
    url: str, 
    output_directory: str,
    proxy_manager: Optional[ProxyManager] = None,
    headless: bool = True
) -> Optional[str]:
    url = url.strip()
    logger.info(f"🎯 download_and_process: url={url[:100]}..., output={output_directory}")
    
    download_dir = os.path.abspath(os.path.join(output_directory, ".downloads"))
    os.makedirs(download_dir, exist_ok=True)
    logger.debug(f"  → Директория загрузок: {download_dir}")
    
    driver = None
    proxy_attempt = 0
    total_proxies = len(proxy_manager.proxies) if proxy_manager else 0
    
    while True:
        # Проверяем, остались ли доступные прокси
        if proxy_manager:
            available = total_proxies - len(proxy_manager.bad_proxies)
            if available <= 0:
                logger.error(f"✗ Все прокси ({total_proxies}) в чёрном списке")
                break
        
        try:
            logger.info(f"  [{'→' if proxy_attempt == 0 else '🔄'}] Попытка #{proxy_attempt + 1} (прокси: {proxy_manager.active_proxy if proxy_manager else 'нет'}, забанено: {len(proxy_manager.bad_proxies) if proxy_manager else 0}/{total_proxies})")
            
            driver = _create_driver_with_proxy(download_dir, proxy_manager, headless)
            
            if not _safe_get(url, driver, proxy_manager=proxy_manager):
                logger.error("✗ Не удалось загрузить страницу")
                if driver:
                    driver.quit()
                    driver = None
                proxy_attempt += 1
                if proxy_manager:
                    proxy_manager.get_working_proxy()
                continue
            
            logger.info(f"✓ Страница загружена: {url}")
            
            wait = WebDriverWait(driver, 20)
            
            logger.debug("→ Поиск вкладки 'Скачать'...")
            tab_download = wait.until(EC.element_to_be_clickable((By.ID, "tabSynopDLoad")))
            driver.execute_script("arguments[0].scrollIntoView(true);", tab_download)
            time.sleep(0.5)
            tab_download.click()
            time.sleep(1)
            logger.debug("  ✓ Вкладка 'Скачать' активирована")
            
            logger.debug("→ Установка даты начала: 01.01.2005")
            start_date_input = wait.until(EC.presence_of_element_located((By.ID, "calender_dload")))
            driver.execute_script("arguments[0].value = '01.01.2005';", start_date_input)
            driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", start_date_input)
            
            logger.debug("→ Выбор формата CSV")
            csv_radio = wait.until(EC.presence_of_element_located((By.ID, "format2")))
            driver.execute_script("arguments[0].click();", csv_radio)
            
            logger.debug("→ Выбор кодировки UTF-8")
            utf8_radio = wait.until(EC.presence_of_element_located((By.ID, "coding2")))
            driver.execute_script("arguments[0].click();", utf8_radio)
            
            logger.debug("→ Клик по кнопке скачивания")
            btn_xpath = "//div[contains(@class, 'archButton') and contains(., 'Select to file GZ')]"
            download_btn = wait.until(EC.element_to_be_clickable((By.XPATH, btn_xpath)))
            driver.execute_script("arguments[0].scrollIntoView(true);", download_btn)
            time.sleep(0.5)
            download_btn.click()
            
            logger.debug("→ Поиск и клик по ссылке скачивания")
            download_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[@id='f_result']/a")))
            href = download_link.get_attribute("href")
            logger.info(f"🔗 Ссылка на скачивание: {href}")
            download_link.click()
            
            logger.info("⏳ Ожидание завершения загрузки (макс. 120с)...")
            downloaded_file = _wait_for_download_complete(download_dir, timeout=120)
            
            if not downloaded_file or not os.path.exists(downloaded_file):
                files_in_dir = os.listdir(download_dir) if os.path.exists(download_dir) else []
                raise FileNotFoundError(
                    f"Архив не найден в {download_dir}. Файлы: {files_in_dir}"
                )
            
            city_name = extract_city_name(url)
            current_date = datetime.now().strftime("%d%m%Y")
            output_filename = f"01012005-{current_date}-{city_name}-data.csv"
            output_path = os.path.abspath(os.path.join(output_directory, output_filename))
            
            logger.info(f"📦 Распаковка: {os.path.basename(downloaded_file)} → {output_filename}")
            
            with gzip.open(downloaded_file, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            os.remove(downloaded_file)
            logger.info(f"✅ Файл сохранён: {output_path}")
            
            if driver:
                driver.quit()
            return output_path
            
        except ProxyRotationNeeded as e:
            logger.warning(f"🔄 Требуется ротация прокси: {e}")
            proxy_attempt += 1
            
            if driver:
                try:
                    driver.quit()
                except:
                    pass
                driver = None
            
            if proxy_manager:
                if proxy_manager.get_working_proxy():
                    logger.info(f"→ Прокси заменён, повторная попытка...")
                    continue
                else:
                    logger.error("✗ Нет доступных прокси для ротации")
            break
                
        except Exception as e:
            logger.error(f"❌ Ошибка в download_and_process: {type(e).__name__}: {e}", exc_info=True)
            
            # Помечаем прокси как плохой при сетевых ошибках
            if proxy_manager and proxy_manager.active_proxy:
                if "proxy" in str(e).lower() or "connection" in str(e).lower() or isinstance(e, (TimeoutException, WebDriverException)):
                    logger.warning("→ Бан прокси из-за ошибки и попытка ротации...")
                    proxy_manager.mark_proxy_bad(proxy_manager.active_proxy)
                    proxy_attempt += 1
                    if driver:
                        try:
                            driver.quit()
                        except:
                            pass
                        driver = None
                    if proxy_manager.get_working_proxy():
                        continue
            
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            break
    
    logger.error("✗ Исчерпаны все попытки загрузки")
    return None


# ============================================================================
# CLI ИНТЕРФЕЙС
# ============================================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Download weather archive from rp5.ru with proxy support"
    )
    parser.add_argument("--url", required=True, help="URL страницы с архивом погоды")
    parser.add_argument("--output_directory", required=True, help="Директория для сохранения CSV")
    parser.add_argument("--proxy-file", type=str, default=None, help="Файл с прокси")
    parser.add_argument("--proxy-timeout", type=float, default=0.5, help="Таймаут теста прокси")
    parser.add_argument("--no-headless", action="store_true", help="Показать браузер (отладка)")
    parser.add_argument("--log-level", type=str, default="DEBUG", 
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Уровень логирования")
    
    logger.debug("  ✓ Отключена проверка SSL-сертификатов")
    args = parser.parse_args()
    
    log_level = getattr(logging, args.log_level.upper())
    logging.getLogger().setLevel(log_level)
    logger.info(f"📝 Уровень логирования: {logging.getLevelName(log_level)}")
    
    proxy_mgr = None
    if args.proxy_file or os.path.exists(ProxyManager.DEFAULT_PROXY_FILE):
        proxy_mgr = ProxyManager(
            proxy_file=args.proxy_file,
            test_timeout=args.proxy_timeout
        )
    
    headless = not args.no_headless
    if headless:
        logger.info("🔒 Браузер запущен в скрытом режиме (headless)")
    
    result = download_and_process(
        url=args.url,
        output_directory=args.output_directory,
        proxy_manager=proxy_mgr,
        headless=headless
    )
    
    if proxy_mgr:
        stats = proxy_mgr.get_stats()
        logger.info(f"📊 Статистика прокси: всего={stats['total']}, забанено={stats['bad']}, доступно={stats['available']}")
    
    if result:
        logger.info(f"🎉 Успешно завершено: {result}")
        sys.exit(0)
    else:
        logger.error("❌ Ошибка выполнения")
        sys.exit(1)