import os
import sys
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Depends, Header
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import logging
import asyncio
from datetime import datetime
import traceback
import shutil
from archive_system import ArchiveSystem
from typing import Optional

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from get_calls import get_call_data, download_call
from transcribe_calls import process_call
from database import db

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Разрешенный IP-адрес UIS
ALLOWED_IP = "195.211.122.249"

# API ключ для авторизации
API_KEY = ""  

app = FastAPI(title="UIS Webhook Server")

# Подключаем статические файлы
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.on_event("startup")
async def startup_event():
    """Выполняется при запуске сервера"""
    logger.info("Webhook server started")
    logger.info(f"Allowed IP: {ALLOWED_IP}")
    logger.info(f"Current directory: {os.getcwd()}")
    logger.info(f"Result directory exists: {os.path.exists('/app/result')}")
    logger.info(f"Data directory exists: {os.path.exists('/app/data')}")

class CallNotification(BaseModel):
    """Модель для входящих данных."""
    communication_id: str

# не используем пока что
async def verify_uis_ip(request: Request):
    """Проверка IP-адреса отправителя."""
    forwarded_for = request.headers.get("X-Forwarded-For")
    logger.info(f"X-Forwarded-For header: {forwarded_for}")
    
 

async def process_unprocessed_calls_from_data(data):
    """Обрабатывает все звонки из выгрузки, которые еще не обработаны (нет transcript_path)"""
    if not data or 'result' not in data or 'data' not in data['result']:
        logger.info("Нет данных для сверки необработанных звонков.")
        return
    all_comm_ids = [str(call.get('communication_id')) for call in data['result']['data']]
    processed_ids = await asyncio.get_event_loop().run_in_executor(None, db.get_processed_communication_ids)
    to_process = [cid for cid in all_comm_ids if cid and cid not in processed_ids]
    if to_process:
        logger.info(f"Найдены необработанные звонки: {to_process}. Запускаю обработку...")
    for comm_id in to_process:
        logger.info(f"Автоматически обрабатываю необработанный звонок {comm_id}")
        await process_call_async(comm_id)

async def process_call_async(comm_id: str, attempt: int = 1) -> dict:
    """Асинхронная обработка звонка. attempt - номер попытки."""
    logger.debug(f"Function process_call_async called with comm_id={comm_id}, attempt={attempt}")
    try:
        logger.info(f"Starting call processing for {comm_id}, attempt {attempt}")
        start_time = datetime.now()
        
        # Получаем общую выгрузку за 2 часа и ищем нужный звонок в ней
        logger.debug(f"Calling get_call_data(None, 120) to get all calls")
        all_calls_data = await asyncio.get_event_loop().run_in_executor(
            None, get_call_data, None, 120
        )
        
        # Ищем нужный звонок в общей выгрузке
        call_data = None
        if all_calls_data and 'result' in all_calls_data and 'data' in all_calls_data['result']:
            for call in all_calls_data['result']['data']:
                if str(call.get('communication_id')) == str(comm_id):
                    call_data = call
                    break
        
        logger.debug(f"Call data: {call_data}")

        # Если звонок не найден или нет дорожек, пробуем повторно (до 2 раз)
        if not call_data or not call_data.get('wav_call_records'):
            if attempt < 2:
                logger.warning(f"No call data or wav_call_records for {comm_id} (attempt {attempt}). Retrying...")
                await asyncio.sleep(2)  # небольшая задержка
                return await process_call_async(comm_id, attempt=attempt+1)
            # После двух попыток — помечаем звонок как NO_WAV
            logger.error(f"No call data or wav_call_records for {comm_id} after 2 attempts. Marking as NO_WAV.")
            # Обновляем БД
            await asyncio.get_event_loop().run_in_executor(
                None, db.update_call_paths, comm_id, None, None, 'NO_WAV'
            )
            # Создаём папку с меткой NO_WAV
            result_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'result')
            folder_name = f'transcribed_call{comm_id}_NO_WAV'
            folder_path = os.path.join(result_dir, folder_name)
            os.makedirs(folder_path, exist_ok=True)
            info_path = os.path.join(folder_path, 'info.txt')
            with open(info_path, 'w', encoding='utf-8') as f:
                f.write(f'Звонок {comm_id}: нет аудиозаписей (wav_call_records) для транскрипции или не найден в API.')
            
            # Сверяем и обрабатываем необработанные звонки из этой выгрузки
            await process_unprocessed_calls_from_data(all_calls_data)
            
            return {
                "success": False,
                "message": "Нет аудиозаписей для транскрипции (NO_WAV)"
            }

        logger.info(f"Call data received for {comm_id}")
        
        # Сохраняем информацию о звонке в БД
        logger.debug(f"Saving call {comm_id} to database")
        await asyncio.get_event_loop().run_in_executor(
            None, db.add_call, comm_id, call_data
        )
        logger.info(f"Call {comm_id} saved to database")

        wav_ids = call_data.get('wav_call_records', [])
        logger.debug(f"wav_ids for {comm_id}: {wav_ids}")
        if len(wav_ids) < 2:
            logger.warning(f"Not enough audio tracks for call {comm_id}")
            return {
                "success": False,
                "message": "Для этого звонка нет двух аудиодорожек"
            }

        # Скачиваем файлы
        logger.info(f"Starting download for call {comm_id}")
        download_start = datetime.now()
        success = await asyncio.get_event_loop().run_in_executor(
            None, download_call, comm_id, wav_ids
        )
        download_elapsed = (datetime.now() - download_start).total_seconds()
        logger.info(f"Download time for {comm_id}: {download_elapsed:.2f} seconds")

        if not success:
            logger.error(f"Failed to download files for call {comm_id}")
            return {
                "success": False,
                "message": "Ошибка при скачивании файлов"
            }

        logger.info(f"Files downloaded successfully for call {comm_id}")
        
        # Проверяем состояние файлов
        result_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'result')
        client_file = os.path.join(result_dir, f'client_{comm_id}.wav')
        staff_file = os.path.join(result_dir, f'staff_{comm_id}.wav')
        for fpath, label in [(client_file, 'client'), (staff_file, 'staff')]:
            if os.path.exists(fpath):
                logger.info(f"{label.capitalize()} file exists: {fpath}, size: {os.path.getsize(fpath)} bytes")
            else:
                logger.warning(f"{label.capitalize()} file missing: {fpath}")

        # Обновляем пути к аудиофайлам в БД
        await asyncio.get_event_loop().run_in_executor(
            None, db.update_call_paths,
            comm_id,
            client_file,
            staff_file
        )

        logger.info(f"Starting transcription for call {comm_id}")
        transcribe_start = datetime.now()

        # Запускаем транскрипцию
        success = await asyncio.get_event_loop().run_in_executor(
            None, process_call, comm_id, client_file, staff_file
        )
        transcribe_elapsed = (datetime.now() - transcribe_start).total_seconds()
        logger.info(f"Transcription time for {comm_id}: {transcribe_elapsed:.2f} seconds")

        if success:
            logger.info(f"Transcription completed for call {comm_id}")
            # Обновляем путь к транскрипции в БД
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            transcript_dir = os.path.join(result_dir, f'transcribed_call{comm_id}_{timestamp}')
            
            await asyncio.get_event_loop().run_in_executor(
                None, db.update_call_paths,
                comm_id,
                None,
                None,
                transcript_dir
            )

            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"Total process_call_async time for {comm_id}: {elapsed:.2f} seconds")
            return {
                "success": True,
                "message": "Звонок успешно обработан и транскрибирован"
            }
        else:
            logger.error(f"Transcription failed for call {comm_id}")
            return {
                "success": False,
                "message": "Ошибка при транскрибации"
            }

    except Exception as e:
        logger.error(f"Error processing call {comm_id}: {str(e)}\n{traceback.format_exc()}", exc_info=True)
        return {
            "success": False,
            "message": f"Ошибка при обработке: {str(e)}"
        }

@app.post("/webhook/call")
async def webhook_handler(
    request: Request,
    notification: CallNotification
):
    """Обработчик webhook-уведомлений от UIS."""
    # Логируем заголовки и тело запроса
    logger.info(f"Headers: {dict(request.headers)}")
    try:
        body = await request.body()
        logger.info(f"Body: {body.decode('utf-8')}")
    except Exception as e:
        logger.warning(f"Could not read request body: {e}")
    comm_id = notification.communication_id
    logger.info(f"Webhook received for call {comm_id}")
    logger.debug(f"CallNotification: {notification}")
    if not comm_id.isdigit():
        logger.warning(f"Invalid communication_id received: {comm_id}")
        raise HTTPException(
            status_code=400,
            detail="communication_id должен быть числом"
        )
    
    # Проверяем, не обрабатывался ли уже этот звонок
    logger.debug(f"Checking if call {comm_id} already exists in DB...")
    existing_call = await asyncio.get_event_loop().run_in_executor(
        None, db.get_call, comm_id
    )
    
    if existing_call and existing_call.get('transcript_path'):
        logger.info(f"Call {comm_id} was already processed")
        logger.debug(f"Existing call info: {existing_call}")
        return {
            "success": True,
            "message": "Звонок уже был обработан ранее",
            "call_info": existing_call
        }
    
    # Запускаем обработку
    logger.info(f"Starting async processing for call {comm_id}")
    start_time = datetime.now()
    result = await process_call_async(comm_id)
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"Total processing time for {comm_id}: {elapsed:.2f} seconds")
    
    if not result["success"]:
        logger.error(f"Failed to process call {comm_id}: {result['message']}")
        raise HTTPException(
            status_code=500,
            detail=result["message"]
        )
    
    # Получаем обновленную информацию о звонке
    call_info = await asyncio.get_event_loop().run_in_executor(
        None, db.get_call, comm_id
    )
    
    logger.info(f"Call {comm_id} successfully processed")
    logger.debug(f"Processed call info: {call_info}")
    return {
        "success": True,
        "message": result["message"],
        "call_info": call_info
    }

@app.get("/call/{comm_id}")
async def get_call_info(comm_id: str):
    """Получение информации о звонке по ID"""
    if not comm_id.isdigit():
        raise HTTPException(
            status_code=400,
            detail="communication_id должен быть числом"
        )

    call_info = await asyncio.get_event_loop().run_in_executor(
        None, db.get_call, comm_id
    )

    if not call_info:
        raise HTTPException(
            status_code=404,
            detail="Звонок не найден"
        )

    return call_info

@app.get("/health")
async def health_check():
    """Эндпоинт для проверки работоспособности сервера."""
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat()
    }

async def verify_api_key(x_api_key: Optional[str] = Header(None)):
    """Проверка API ключа"""
    if x_api_key != API_KEY:
        raise HTTPException(
            status_code=401, 
            detail="Неверный API ключ. Используйте заголовок X-API-Key"
        )
    return x_api_key

@app.get("/api/export/analysis/{date_from}/{date_to}")
async def export_analysis_data(
    date_from: str, 
    date_to: str, 
    include_audio: bool = True,
    api_key: str = Depends(verify_api_key)
):
    """Экспорт данных для анализа за период"""
    try:
        logger.info(f"Запрос на экспорт данных с {date_from} по {date_to} (авторизован)")
        
        archive_system = ArchiveSystem()
        export_path = archive_system.create_analysis_export(date_from, date_to, include_audio)
        
        return FileResponse(
            export_path,
            filename=f"analysis_export_{date_from}_{date_to}.tar.gz",
            media_type='application/gzip'
        )
    except Exception as e:
        logger.error(f"Ошибка при создании экспорта: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при создании экспорта: {str(e)}")

@app.post("/api/archive/old-calls")
async def archive_old_calls(
    days_old: int = 7,
    api_key: str = Depends(verify_api_key)
):
    """Архивирование старых звонков"""
    try:
        logger.info(f"Запрос на архивирование звонков старше {days_old} дней (авторизован)")
        
        archive_system = ArchiveSystem()
        archive_system.archive_old_calls(days_old)
        
        return {
            "success": True,
            "message": f"Архивирование звонков старше {days_old} дней завершено"
        }
    except Exception as e:
        logger.error(f"Ошибка при архивировании: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при архивировании: {str(e)}")

@app.get("/api/stats")
async def get_stats(api_key: str = Depends(verify_api_key)):
    """Получение статистики по звонкам"""
    try:
        # Получаем статистику из БД
        total_calls = len(db.get_calls_for_analysis("2020-01-01", "2030-12-31"))
        archived_calls = len([call for call in db.get_calls_for_analysis("2020-01-01", "2030-12-31") if call.get('is_archived')])
        
        return {
            "total_calls": total_calls,
            "archived_calls": archived_calls,
            "active_calls": total_calls - archived_calls,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении статистики: {str(e)}")

@app.get("/", response_class=HTMLResponse)
async def get_web_interface():
    """Главная страница веб-интерфейса"""
    try:
        with open("static/index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="""
        <html>
        <head><title>Транскрипция звонков</title></head>
        <body>
            <h1>Транскрипция звонков</h1>
            <p>Веб-интерфейс не найден. Проверьте, что файл static/index.html существует.</p>
        </body>
        </html>
        """, status_code=404)

if __name__ == "__main__":
    uvicorn.run(
        "webhook_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True  
    ) 