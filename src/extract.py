import os
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
import io
from airflow.models import Variable

KEY_PATH = "/opt/airflow/config/winter-berm-501114-n0-b467bbbd03ef.json"
LOCAL_PATH = "/opt/airflow/data/dataEngineeringDataset.csv"


def download_from_drive():
    file_id = Variable.get("gdrive_file_id")
    if os.path.exists(LOCAL_PATH):
        print(f"Fajl vec postoji na {LOCAL_PATH}, preskacem ponovno skidanje fajla")
        return LOCAL_PATH
    
    creds = service_account.Credentials.from_service_account_file(KEY_PATH, scopes = ["https://www.googleapis.com/auth/drive.readonly"])

    service = build("drive", "v3", credentials = creds)

    request = service.files().get_media(fileId=file_id)
    with open(LOCAL_PATH, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Skunuto {int(status.progress() * 100)}%")

    return LOCAL_PATH