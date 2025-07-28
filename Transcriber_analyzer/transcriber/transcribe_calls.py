import json
import os
import requests
import soundfile as sf
import numpy as np
from datetime import datetime
import re

# OpenAI API Key
OPENAI_API_KEY = "*"

# Proxy configuration
HTTP_PROXY = {
    'http': '*',
    'https': '*'
}

SOCKS5_PROXY = {
    'http': '*',
    'https': '*'
}


CURRENT_PROXY = HTTP_PROXY

def create_session():
    session = requests.Session()
    session.proxies = CURRENT_PROXY
    return session

def transcribe_audio(audio_file):
    try:
        session = create_session()
        
        # Prepare the audio file
        with open(audio_file, 'rb') as f:
            files = {'file': f}
            headers = {
                'Authorization': f'Bearer {OPENAI_API_KEY}'
            }
            
            # отправляем запрос через прокси 
            response = session.post(
                'https://api.openai.com/v1/audio/transcriptions',
                headers=headers,
                files=files,
                data={
                    'model': 'whisper-1',
                    'response_format': 'verbose_json',
                    'language': 'ru'
                },
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error: {response.status_code}")
                print(f"Response: {response.text}")
                return None
    except Exception as e:
        print(f"Error transcribing {audio_file}: {str(e)}")
        return None

def format_time(seconds):
    minutes = int(seconds // 60)
    seconds_part = seconds % 60
    return f"{minutes:02d}:{seconds_part:05.2f}"

def check_existing_transcription(result_dir, comm_id):
    existing = [d for d in os.listdir(result_dir) 
               if d.startswith(f'transcribed_call{comm_id}_') 
               and os.path.isdir(os.path.join(result_dir, d))]
    
    if existing:
        print(f"Found existing transcription(s) for call {comm_id}:")
        for folder in existing:
            print(f"  - {folder}")
            folder_path = os.path.join(result_dir, folder)
            files = os.listdir(folder_path)
            if all(f in files for f in ['dialog.txt', 'client_transcript.json', 'staff_transcript.json']):
                print("    Transcription is complete, skipping...")
                return True
            else:
                print("    Transcription appears incomplete, will retranscribe...")
    return False

def create_call_folder(comm_id):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"transcribed_call{comm_id}_{timestamp}"
    folder_path = os.path.join('result', folder_name)
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

def merge_transcripts(client_transcript, staff_transcript):
    segments = []
    
    if client_transcript and 'segments' in client_transcript:
        for segment in client_transcript['segments']:
            segments.append({
                'start': segment['start'],
                'text': segment['text'].strip(),
                'speaker': 'Клиент'
            })
    
    if staff_transcript and 'segments' in staff_transcript:
        for segment in staff_transcript['segments']:
            segments.append({
                'start': segment['start'],
                'text': segment['text'].strip(),
                'speaker': 'Сотрудник'
            })
    
    segments.sort(key=lambda x: x['start'])
    return segments

def save_dialog_format(client_transcript, staff_transcript, output_file):
    segments = merge_transcripts(client_transcript, staff_transcript)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for segment in segments:
            timestamp = format_time(segment['start'])
            f.write(f"[{timestamp}] {segment['speaker']}: {segment['text']}\n")

def get_comm_id_from_filename(filename):
    match = re.search(r'(?:client|staff)_(\d+)\.wav', filename)
    return match.group(1) if match else None

def process_call(comm_id, client_file, staff_file):
    """
    Обработка конкретного звонка.
    Args:
        comm_id: ID звонка
        client_file: путь к файлу клиента
        staff_file: путь к файлу сотрудника
    Returns:
        bool: True если транскрипция успешна, False в противном случае
    """
    if not (os.path.exists(client_file) and os.path.exists(staff_file)):
        print(f"Missing audio files for call {comm_id}")
        return False
    
    
    if check_existing_transcription('result', comm_id):
        return True
    
    print(f"Transcribing call {comm_id}")
    print(f"  Client: {os.path.basename(client_file)}")
    print(f"  Staff:  {os.path.basename(staff_file)}")
    
    
    call_folder = create_call_folder(comm_id)
    

    client_transcript = transcribe_audio(client_file)
    staff_transcript = transcribe_audio(staff_file)
    
    if client_transcript and staff_transcript:
        with open(os.path.join(call_folder, 'client_transcript.json'), 'w', encoding='utf-8') as f:
            json.dump(client_transcript, f, ensure_ascii=False, indent=2)
        
        with open(os.path.join(call_folder, 'staff_transcript.json'), 'w', encoding='utf-8') as f:
            json.dump(staff_transcript, f, ensure_ascii=False, indent=2)
        
        save_dialog_format(
            client_transcript, 
            staff_transcript, 
            os.path.join(call_folder, 'dialog.txt')
        )
        
        print(f"Transcription saved in: {call_folder}")
        return True
    
    print(f"Failed to transcribe call {comm_id}")
    return False

def main(specific_comm_id=None):
    """
    Основная функция. Может работать в двух режимах:
    1. Без параметров - транскрибирует все файлы в директории result
    2. С specific_comm_id - транскрибирует только указанный звонок
    """
    result_dir = 'result'
    if not os.path.exists(result_dir):
        print(f"Error: {result_dir} directory not found")
        return

    if specific_comm_id:
        # Режим обработки конкретного звонка
        client_file = os.path.join(result_dir, f'client_{specific_comm_id}.wav')
        staff_file = os.path.join(result_dir, f'staff_{specific_comm_id}.wav')
        
        if process_call(specific_comm_id, client_file, staff_file):
            print(f"Successfully transcribed call {specific_comm_id}")
        else:
            print(f"Failed to transcribe call {specific_comm_id}")
        return

    # Стандартный режим - обработка всех файлов
    client_files = [f for f in os.listdir(result_dir) if f.startswith('client_') and f.endswith('.wav')]
    
    for client_file in client_files:
        comm_id = get_comm_id_from_filename(client_file)
        if not comm_id:
            print(f"Warning: Could not extract comm_id from {client_file}")
            continue
            
        staff_file = f'staff_{comm_id}.wav'
        
        
        client_path = os.path.join(result_dir, client_file)
        staff_path = os.path.join(result_dir, staff_file)
        
        process_call(comm_id, client_path, staff_path)

if __name__ == "__main__":
    import sys
    # Если передан аргумент - считаем его communication_id
    specific_comm_id = sys.argv[1] if len(sys.argv) > 1 else None
    main(specific_comm_id) 