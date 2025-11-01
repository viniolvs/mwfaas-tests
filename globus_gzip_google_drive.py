import os
import sys
from typing import Dict, List, Any

from mwfaas.master import Master
from mwfaas.globus_compute_manager import GlobusComputeCloudManager
from mwfaas.list_distribuition_strategy import ListDistributionStrategy

from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


def worker_function(files: List[dict[str, Any]], metadata: Dict[str, Any]):
    import io
    import json
    import gzip
    from typing import Any
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import (
        MediaIoBaseDownload,
        MediaIoBaseUpload,
    )

    # return metadata

    if not files or len(files) == 0:
        return []

    SCOPES = ["https://www.googleapis.com/auth/drive"]

    # --- Funções Auxiliares ---

    def get_drive_service():
        """Autentica e retorna o objeto de serviço da API do Drive."""
        token_json_string = metadata.get("token")
        if not token_json_string:
            raise ValueError(
                "Erro no Worker: O parâmetro 'metadata' não continha a chave 'google_auth_token_json' necessária para a autenticação no Google Drive."
            )

        creds = None
        try:
            token_info = json.loads(token_json_string)
            creds = Credentials.from_authorized_user_info(token_info, SCOPES)
            if creds and creds.expired and creds.refresh_token:
                print("[Worker] Token expirado, tentando atualizar...")
                creds.refresh(Request())

        except Exception as e:
            print(f"Um erro ocorreu ao carregar as credenciais do metadata: {e}")
            return None

        try:
            service = build("drive", "v3", credentials=creds)
            print("[Worker] Serviço do Google Drive autenticado com sucesso.")
            return service
        except HttpError as error:
            print(f"Um erro ocorreu ao construir o serviço: {error}")
            return None

    def download_file_to_memory(service, file_id) -> io.BytesIO:
        """Baixa um arquivo do Google Drive para um buffer em memória."""
        try:
            request = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            print(f"Iniciando download em memória do arquivo ID: {file_id}...")
            while done is False:
                status, done = downloader.next_chunk()
            fh.seek(0)
            return fh
        except HttpError as error:
            print(f"Um erro ocorreu no download para memória: {error}")
            raise error

    def upload_bytes_to_drive(
        service, file_bytes: bytes, new_filename: str, folder_id=None
    ):
        """Faz upload de um objeto de bytes para o Google Drive."""
        try:
            file_metadata: dict[str, Any] = {"name": new_filename}
            if folder_id:
                file_metadata["parents"] = [folder_id]

            # Cria um buffer de bytes para o upload
            fh = io.BytesIO(file_bytes)

            media = MediaIoBaseUpload(
                fh,
                mimetype="application/gzip",
                resumable=True,
            )

            print(f"Iniciando upload em memória de: {new_filename}...")
            file = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id, name")
                .execute()
            )
            print(
                f"Upload em memória concluído! Nome: {file.get('name')}, ID: {file.get('id')}"
            )
            return file.get("id")
        except HttpError as error:
            print(f"Um erro ocorreu no upload dos bytes: {error}")
            raise error

    # --- Lógica Principal do Worker ---

    drive_service = get_drive_service()
    if not drive_service:
        raise Exception("Erro ao autenticar e construir o serviço do Google Drive.")

    folder_id = metadata.get("folder_id")
    if not folder_id:
        raise Exception(
            "Erro no Worker: O parâmetro 'metadata' não continha a chave 'folder_id' necessária para o upload."
        )

    results = []

    # file_ids é o 'chunk' que o Master enviou
    for file in files:
        file_name = file["name"]
        file_id = file["id"]
        try:
            print(f"[Worker] Processando file: {file_name}...")
            downloaded_bytes_buffer = download_file_to_memory(drive_service, file_id)

            print(f"[Worker] Compactando {file_name} em memória...")
            uncompressed_data = downloaded_bytes_buffer.getvalue()
            compressed_data = gzip.compress(uncompressed_data)

            # Upload
            new_filename = f"{file_name}.gz"
            new_file_id = upload_bytes_to_drive(
                drive_service,
                compressed_data,
                new_filename,
                folder_id=folder_id,
            )

            results.append(
                {"original_id": file_id, "new_id": new_file_id, "status": "success"}
            )

        except Exception as e:
            print(f"[Worker] Falha ao processar {file_name}: {e}")
            results.append(
                {
                    "original_id": file_id,
                    "original_name": file_name,
                    "new_id": None,
                    "new_name": None,
                    "status": "failed",
                    "error": str(e),
                }
            )

    return results


def google_drive_auth():
    """Autentica e retorna o objeto de serviço da API do Drive."""
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("drive", "v3", credentials=creds)
        return service
    except HttpError as error:
        print(f"Um erro ocorreu ao construir o serviço: {error}")
        return None


def list_files_in_folder(service, folder_id):
    """
    Lista todos os arquivos e subpastas dentro de uma pasta específica do Drive.

    Args:
        service: Objeto de serviço da API do Drive.
        folder_id (str): O ID da pasta do Drive a ser pesquisada.

    Returns:
        list: Uma lista de dicionários, onde cada dicionário contém 'id' e 'name' do arquivo.
    """
    all_files = []
    page_token = None
    try:
        print(f"Listando arquivos da pasta ID: {folder_id}...")
        while True:
            # A query 'q' filtra pela pasta pai
            # fields='nextPageToken, files(id, name)' pede apenas os campos que queremos
            response = (
                service.files()
                .list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    spaces="drive",
                    fields="nextPageToken, files(id, name)",
                    pageToken=page_token,
                )
                .execute()
            )

            files = response.get("files", [])
            all_files.extend(files)

            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break  # Sai do loop quando não há mais páginas

        print(f"Encontrados {len(all_files)} arquivos.")
        return all_files

    except HttpError as error:
        print(f"Um erro ocorreu ao listar os arquivos: {error}")
        return []


def main():
    service = google_drive_auth()
    if not service:
        return

    folder_id = sys.argv[1]
    files = list_files_in_folder(service=service, folder_id=folder_id)
    print(f"files: {files}")

    with open("token.json", "r") as f:
        # Lê o conteúdo inteiro do arquivo (que é uma string JSON)
        token_json_string = f.read()

    metadata = {"folder_id": folder_id, "token": token_json_string}

    with GlobusComputeCloudManager(auto_authenticate=True) as cloud_manager:
        distribuition = ListDistributionStrategy()
        master = Master(
            cloud_manager=cloud_manager, distribution_strategy=distribuition
        )

        try:
            results = master.run(
                data_input=files,
                user_function=worker_function,
                metadata=metadata,
            )
            print(f"results: {results}")

        except Exception as e:
            print(
                f"\nOcorreu um erro durante a execução do master.run: {type(e).__name__} - {e}"
            )

        print("\n" + "-" * 15 + " Status das Tarefas " + "-" * 15)
        task_statuses = master.get_task_statuses()
        if task_statuses:
            for status in task_statuses:
                status_info = (
                    f"  ID da Tarefa: {status.get('id', 'N/A'):<38} "
                    f"Worker ID: {status.get('worker_id', 'N/A')}"
                    f"Índice do Bloco: {status.get('chunk_index', 'N/A'):<3} "
                    f"Status: {status.get('status', 'N/A'):<20}"
                )
                print(status_info)
        else:
            print("Nenhum status de tarefa disponível.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("globus_gzip_google_drive <folder_id>")
        sys.exit(1)
    else:
        main()
