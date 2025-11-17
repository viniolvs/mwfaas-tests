import argparse
import math
import os
import time
from typing import Any, Dict, List

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from mwfaas.globus_compute_manager import GlobusComputeCloudManager
from mwfaas.list_distribuition_strategy import ListDistributionStrategy
from mwfaas.master import Master


def worker_function(files: List[dict[str, Any]], metadata: Dict[str, Any]):
    import gzip
    import io
    import json
    import time
    from typing import Any

    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import (
        MediaIoBaseDownload,
        MediaIoBaseUpload,
    )

    all_time_start = time.perf_counter()

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
    for file in files:
        start_time = time.perf_counter()
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

            end_time = time.perf_counter()
            results.append(
                {
                    "original_id": file_id,
                    "new_id": new_file_id,
                    "status": "success",
                    "time": end_time - start_time,
                }
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

    all_time_end = time.perf_counter()
    return {"data": results, "time": all_time_end - all_time_start}


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
                break

        print(f"Encontrados {len(all_files)} arquivos.")
        return all_files

    except HttpError as error:
        print(f"Um erro ocorreu ao listar os arquivos: {error}")
        return []


def main_local(folder_id, output_folder_id):
    print("[Local] Executando script localmente...")
    service = google_drive_auth()
    if not service:
        return

    files = list_files_in_folder(service=service, folder_id=folder_id)

    with open("token.json", "r") as f:
        token_json_string = f.read()

    metadata = {"folder_id": output_folder_id, "token": token_json_string}
    start_time = time.perf_counter()
    res = worker_function(files, metadata)
    end_time = time.perf_counter()

    if not isinstance(res, dict) or not res.get("data"):
        print(f"[Local] Erro ao processar os arquivos: {res}")
        return

    results = res.get("data", [])

    print(f"[Local] Tempo de execução total: {end_time - start_time:.4f} segundos")
    print(f"results: {results}")
    execution_times = []
    for result in results:
        if isinstance(result, dict):
            execution_times.append(result.get("time", 0))
    if execution_times:
        print(
            f"\n[Local] Tempo médio de execução por worker: {sum(execution_times) / len(execution_times):.4f}s"
        )
        print(
            f"[Local] Tempo máximo de execução de um worker: {max(execution_times):.4f}s"
        )
        print(
            f"[Local] Tempo mínimo de execução de um worker: {min(execution_times):.4f}s"
        )
        print("[Local] Execution times:", execution_times)


def main(folder_id: str, output_folder_id: str, one_per_worker: bool):
    service = google_drive_auth()
    if not service:
        return

    files = list_files_in_folder(service=service, folder_id=folder_id)

    with open("token.json", "r") as f:
        token_json_string = f.read()

    metadata = {"folder_id": output_folder_id, "token": token_json_string}

    with GlobusComputeCloudManager(auto_authenticate=True) as cloud_manager:
        worker_count = len(cloud_manager.available_endpoint_ids)
        print(f"Número de workers disponíveis: {worker_count}")
        items_per_worker = 1
        if not one_per_worker:
            items_per_worker = math.ceil(len(files) / worker_count)

        print(f"items_per_worker: {items_per_worker}")
        distribuition = ListDistributionStrategy(items_per_worker)
        master = Master(
            cloud_manager=cloud_manager, distribution_strategy=distribuition
        )

        try:
            start_time = time.perf_counter()
            results = master.run(
                data_input=files,
                user_function=worker_function,
                metadata=metadata,
            )
            end_time = time.perf_counter()
            print(
                f"Tempo de execução master.run(): {end_time - start_time:.4f} segundos"
            )
            print(f"results: {results}")
            execution_times = []
            for result in results:
                if isinstance(result, dict):
                    execution_times.append(result.get("time", 0))
            if execution_times:
                print(
                    f"\n[Master] Tempo médio de execução por worker: {sum(execution_times) / len(execution_times):.4f}s"
                )
                print(
                    f"[Master] Tempo máximo de execução de um worker: {max(execution_times):.4f}s"
                )
                print(
                    f"[Master] Tempo mínimo de execução de um worker: {min(execution_times):.4f}s"
                )
                print("[Master] Execution times:", execution_times)

            print("\n" + "-" * 15 + " Status das Tarefas " + "-" * 15)
            print(master.get_task_statuses())

        except Exception as e:
            print(
                f"\nOcorreu um erro durante a execução do master.run: {type(e).__name__} - {e}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Processa arquivos do Google Drive, opcionalmente via Globus."
    )

    parser.add_argument(
        "folder_id",
        type=str,
        help="ID da pasta de origem no Google Drive (obrigatório)",
    )

    parser.add_argument(
        "output_folder_id",
        type=str,
        nargs="?",  # '?' significa 0 ou 1 argumento
        default=None,
        help="ID da pasta de destino (opcional, padrão: mesmo ID da origem)",
    )

    parser.add_argument(
        "--run_local",
        action="store_true",
        help="Se presente, executa o script localmente (padrão: executa no Globus)",
    )

    parser.add_argument(
        "--one_per_worker",
        action="store_true",
    )

    args = parser.parse_args()

    folder_id = args.folder_id
    run_local = args.run_local
    one_per_worker = args.one_per_worker

    output_folder_id = args.output_folder_id
    if output_folder_id is None:
        output_folder_id = folder_id

    if run_local:
        main_local(folder_id, output_folder_id)
    else:
        main(folder_id, output_folder_id, one_per_worker)
