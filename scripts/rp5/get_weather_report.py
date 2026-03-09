import re
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout


def download_weather_report(
    page_url: str,
    output_dir: str = "./downloads",
    start_date: str = "01.01.2010",
    end_date: str = None,
    timeout_ms: int = 30000
) -> str | None:
    """
    Скачивает архив погодных данных в формате CSV (UTF-8) с указанного сайта.
    
    :param page_url: URL страницы с формой выбора параметров
    :param output_dir: Директория для сохранения файла
    :param start_date: Начальная дата в формате ДД.ММ.ГГГГ
    :param end_date: Конечная дата в формате ДД.ММ.ГГГГ (по умолчанию - сегодня)
    :param timeout_ms: Таймаут ожидания элементов в миллисекундах
    :return: Путь к скачанному файлу или None в случае ошибки
    """
    
    if end_date is None:
        from datetime import datetime
        end_date = datetime.now().strftime("%d.%m.%Y")
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            accept_downloads=True
        )
        context.set_default_timeout(timeout_ms)
        page = context.new_page()
        
        try:
            print(f"[+] Переход на страницу: {page_url}")
            page.goto(page_url, wait_until="networkidle")
            
            print(f"[+] Установка дат: {start_date} — {end_date}")
            page.fill('input[name="ArchDate1"]', start_date)
            page.fill('input[name="ArchDate2"]', end_date)
            page.locator('input[name="ArchDate1"]').dispatch_event("change")
            page.locator('input[name="ArchDate2"]').dispatch_event("change")
            
            print("[+] Выбор опции: все дни")
            page.locator('input[name="f_pe"][value="1"]').check(force=True)
        
            print("[+] Выбор формата: CSV")
            page.locator('input[name="format"][value="f_csv"]').check(force=True)
            
            print("[+] Выбор кодировки: UTF-8")
            page.locator('input[name="f_pe1"][value="2"]').check(force=True)
            
            page.wait_for_timeout(500)
            
            print("[+] Генерация ссылки на скачивание...")
            download_button = page.locator('div.archButton:has-text("Выбрать в файл GZ")').first
            
            with page.expect_download(timeout=timeout_ms) as download_info:
                download_button.click(force=True)
                download = download_info.value
                
                suggested_filename = download.suggested_filename
                if not suggested_filename:
                    suggested_filename = f"weather_{start_date.replace('.', '_')}_{end_date.replace('.', '_')}.csv.gz"
                
                output_path = Path(output_dir) / suggested_filename
                print(f"[+] Скачивание файла: {suggested_filename}")
                download.save_as(str(output_path))
                
                if output_path.stat().st_size > 0:
                    print(f"[✓] Файл успешно сохранён: {output_path.resolve()}")
                    return str(output_path.resolve())
                else:
                    print("[✗] Файл пустой или не был скачан")
                    return None
                    
        except PlaywrightTimeout:
            print(f"[✗] Таймаут при ожидании элемента (>{timeout_ms}мс)")
            debug_path = Path(output_dir) / "debug_timeout.png"
            page.screenshot(path=str(debug_path))
            print(f"[i] Скриншот сохранён: {debug_path}")
            return None
        except Exception as e:
            print(f"[✗] Ошибка: {type(e).__name__}: {e}")
            debug_path = Path(output_dir) / "debug_error.png"
            page.screenshot(path=str(debug_path))
            return None
        finally:
            browser.close()


if __name__ == "__main__":
    TEST_URL = "https://ru6.rp5.ru/Weather_archive_on_the_ground/28698"
    
    result = download_weather_report(
        page_url=TEST_URL,
        output_dir="./weather_data",
        start_date="01.01.2005"
    )
    
    if result:
        print(f"\n✅ Готово! Файл: {result}")
    else:
        print("\n❌ Не удалось скачать файл")