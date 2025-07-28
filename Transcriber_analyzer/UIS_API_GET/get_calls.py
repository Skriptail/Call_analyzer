import os
import json
import requests
from datetime import datetime, timedelta
import time

ACCESS_TOKEN = '*'

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def get_call_data(comm_id=None, minutes=10):
    """
    Получает данные о звонках. Если указан comm_id, ищет конкретный звонок за последние minutes минут.
    Если comm_id не указан, возвращает все звонки за последние 24 часа.
    """
    # Если ищем конкретный звонок - смотрим за последние 120 минут и увеличиваем лимит
    if comm_id:
        minutes = 120
        limit = 1000
    else:
        limit = 100
    date_till = datetime.now()
    date_from = date_till - timedelta(minutes=minutes if comm_id else 1440)
    
    url = 'https://dataapi.comagic.ru/v2.0'
    headers = {'Content-Type': 'application/json'}
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "get.calls_report",
        "params": {
            "access_token": ACCESS_TOKEN,
            "date_from": date_from.strftime('%Y-%m-%d %H:%M:%S'),
            "date_till": date_till.strftime('%Y-%m-%d %H:%M:%S'),
            "limit": limit,
            "offset": 0,
        }
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        data = response.json()
        if comm_id:
            # Если ищем конкретный звонок, возвращаем только его данные
            found = False
            comm_ids = []
            for call in data.get('result', {}).get('data', []):
                comm_ids.append(str(call.get('communication_id')))
                if str(call.get('communication_id')) == str(comm_id):
                    found = True
                    return call
            if not found:
                print(f"[get_call_data] Звонок {comm_id} не найден. Список communication_id в выгрузке: {comm_ids}")
                # Логируем весь json-ответ (обрезаем если длинный)
                data_str = json.dumps(data, ensure_ascii=False)
                if len(data_str) > 2000:
                    data_str = data_str[:2000] + '... [truncated]'
                print(f"[get_call_data] Полный ответ API при поиске звонка {comm_id}: {data_str}")
            return None
        return data
    else:
        print(f"Ошибка: {response.status_code}")
        print(response.text)
        return None

def find_call_with_retries(comm_id, minutes=10, retries=3, delay=300):
    """
    Пытается найти звонок с comm_id несколько раз с задержкой между попытками.
    :param comm_id: ID звонка
    :param minutes: За сколько минут искать звонок
    :param retries: Количество попыток
    :param delay: Задержка между попытками (секунды)
    :return: Данные звонка или None
    """
    for attempt in range(1, retries + 1):
        log(f"Попытка {attempt} найти звонок {comm_id} за последние {minutes} минут...")
        call_data = get_call_data(comm_id, minutes)
        if call_data:
            log(f"Звонок {comm_id} найден на попытке {attempt}")
            return call_data
        else:
            log(f"Звонок {comm_id} не найден. Ожидание {delay} секунд перед следующей попыткой...")
            if attempt < retries:
                time.sleep(delay)
    log(f"Звонок {comm_id} не найден после {retries} попыток.")
    return None

def download_call(comm_id, wav_ids, result_dir='result'):
    """Скачивает аудиозаписи конкретного звонка."""
    log(f"Начинаю загрузку аудиозаписей для звонка {comm_id}...")
    os.makedirs(result_dir, exist_ok=True)
    
    if len(wav_ids) < 2:
        log(f'Нет двух дорожек для звонка {comm_id} (wav_ids: {wav_ids})')
        return False
        
    url_template = 'https://app.comagic.ru/system/media/wav/{comm_id}/{wav_id}/'
    client_wav_url = url_template.format(comm_id=comm_id, wav_id=wav_ids[0])
    staff_wav_url = url_template.format(comm_id=comm_id, wav_id=wav_ids[1])
    
    client_wav_filename = os.path.join(result_dir, f'client_{comm_id}.wav')
    staff_wav_filename = os.path.join(result_dir, f'staff_{comm_id}.wav')
    
    # Проверяем, существуют ли файлы и имеют ли они ненулевой размер
    files_exist = True
    for fname, who in [(client_wav_filename, 'Клиент'), (staff_wav_filename, 'Сотрудник')]:
        if os.path.exists(fname) and os.path.getsize(fname) > 0:
            log(f'Файл {who} для звонка {comm_id} уже существует: {fname} (размер: {os.path.getsize(fname)} байт)')
        else:
            files_exist = False
            break
    
    if files_exist:
        log(f'Все файлы для звонка {comm_id} уже существуют, пропускаю скачивание')
        return True
    
    success = True
    for url_, fname, who in [
        (client_wav_url, client_wav_filename, 'Клиент'),
        (staff_wav_url, staff_wav_filename, 'Сотрудник')
    ]:
        # Проверяем, нужно ли скачивать этот файл
        if os.path.exists(fname) and os.path.getsize(fname) > 0:
            log(f'Файл {who} для звонка {comm_id} уже существует, пропускаю: {fname}')
            continue
            
        log(f"Пробую скачать {who} ({url_}) для звонка {comm_id}...")
        try:
            r = requests.get(url_, stream=True)
            if r.status_code == 200:
                with open(fname, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                # Проверяем размер скачанного файла
                file_size = os.path.getsize(fname)
                log(f'{who} для звонка {comm_id} сохранён: {fname} (размер: {file_size} байт)')
                if file_size == 0:
                    log(f'ВНИМАНИЕ: Файл {who} для звонка {comm_id} имеет нулевой размер!')
                    success = False
            else:
                log(f'Ошибка загрузки {who} ({url_}) для звонка {comm_id}: HTTP {r.status_code}')
                success = False
        except Exception as e:
            log(f'Исключение при загрузке {who} ({url_}) для звонка {comm_id}: {e}')
            success = False
    
    return success

def main(specific_comm_id=None):
    """
    Основная функция. Может работать в двух режимах:
    1. Без параметров - скачивает все звонки за последние 24 часа
    2. С specific_comm_id - скачивает конкретный звонок
    """
    if specific_comm_id:
        # Режим скачивания конкретного звонка с повторными попытками
        call_data = find_call_with_retries(specific_comm_id, minutes=10, retries=3, delay=300)
        if call_data:
            wav_ids = call_data.get('wav_call_records', [])
            if download_call(specific_comm_id, wav_ids):
                log(f'Звонок {specific_comm_id} успешно загружен')
            else:
                log(f'Ошибка при загрузке звонка {specific_comm_id}')
        else:
            log(f'Звонок {specific_comm_id} не найден после всех попыток')
        return

    # Стандартный режим - загрузка всех звонков
    data = get_call_data()
    if not data:
        log('Ошибка получения данных о звонках')
        return
        
    # Сохраняем json-отчёт
    with open('calls_report.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log('calls_report.json сохранён.')
    
    items = data.get('result', {}).get('data', [])
    if not items:
        log('Нет записей для обработки.')
        return
        
    for item in items:
        comm_id = item.get('communication_id')
        wav_ids = item.get('wav_call_records', [])
        download_call(comm_id, wav_ids)

if __name__ == "__main__":
    import sys
    # Если передан аргумент - считаем его communication_id
    specific_comm_id = sys.argv[1] if len(sys.argv) > 1 else None
    main(specific_comm_id) 