import os
import sys
import argparse
import io
import mimetypes
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# Escopo de permissão total
SCOPES = ["https://www.googleapis.com/auth/drive"]


def get_drive_service():
    """Autentica e retorna o objeto de serviço da API do Drive."""
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


# --- Função para Baixar (da resposta anterior) ---
def download_file(service, file_id, local_destination):
    """Baixa um arquivo do Google Drive."""
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        print(f"Iniciando download do arquivo ID: {file_id}...")
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%.")
        fh.seek(0)
        with open(local_destination, "wb") as f:
            f.write(fh.read())
        print(f"Arquivo baixado e salvo em: {local_destination}")
        return True
    except HttpError as error:
        print(f"Um erro ocorreu no download: {error}")
        return False


# --- Função de Upload (da resposta anterior, usada pela nova função) ---
def upload_file(service, local_filepath, mime_type, folder_id=None):
    """Faz upload de UM arquivo para o Google Drive."""
    try:
        file_metadata = {"name": os.path.basename(local_filepath)}
        if folder_id:
            file_metadata["parents"] = [folder_id]

        # Tenta adivinhar o mime_type se não for fornecido um válido
        if not mime_type:
            mime_type, _ = mimetypes.guess_type(local_filepath)
            if not mime_type:
                mime_type = "application/octet-stream"  # Tipo genérico

        media = MediaFileUpload(local_filepath, mimetype=mime_type, resumable=True)

        print(f"Iniciando upload de: {local_filepath}...")

        file = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id, name")
            .execute()
        )

        print(f"Upload concluído! Nome: {file.get('name')}, ID: {file.get('id')}")
        return file.get("id")

    except HttpError as error:
        print(f"Um erro ocorreu no upload de {local_filepath}: {error}")
        return None
    except FileNotFoundError:
        print(f'Erro: O arquivo local "{local_filepath}" não foi encontrado.')
        return None


# --- NOVA FUNÇÃO 1: Listar Arquivos em um Diretório ---


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


# --- NOVA FUNÇÃO 2: Hospedar Vários Arquivos e Obter IDs ---


def upload_multiple_files(service, local_folder_path, drive_folder_id):
    """
    Faz upload de todos os arquivos de um diretório local para uma pasta do Drive.

    Args:
        service: Objeto de serviço da API do Drive.
        local_folder_path (str): O caminho para a pasta local (ex: './meus_arquivos').
        drive_folder_id (str): O ID da pasta de destino no Drive.

    Returns:
        dict: Um dicionário mapeando o nome do arquivo local ao seu novo ID no Drive.
              Ex: {'relatorio.pdf': '1a2b3c...', 'dados.csv': '4d5e6f...'}
    """
    uploaded_file_ids = {}

    try:
        print(
            f"Iniciando upload da pasta local '{local_folder_path}' para a pasta Drive ID '{drive_folder_id}'"
        )

        # Itera sobre todos os arquivos no diretório local
        for filename in os.listdir(local_folder_path):
            local_filepath = os.path.join(local_folder_path, filename)

            # Verifica se é um arquivo (e não uma subpasta)
            if os.path.isfile(local_filepath):
                # Adivinha o tipo MIME
                mime_type, _ = mimetypes.guess_type(local_filepath)
                if not mime_type:
                    mime_type = "application/octet-stream"  # Tipo genérico

                # Reutiliza nossa função de upload de arquivo único
                file_id = upload_file(
                    service, local_filepath, mime_type, drive_folder_id
                )

                if file_id:
                    # Armazena o ID em tempo de execução
                    uploaded_file_ids[filename] = file_id
            else:
                print(f"Ignorando '{filename}' (não é um arquivo).")

        print("\nUpload em lote concluído.")
        return uploaded_file_ids

    except FileNotFoundError:
        print(f"Erro: A pasta local '{local_folder_path}' não foi encontrada.")
        return {}
    except HttpError as error:
        print(f"Um erro ocorreu durante o upload em lote: {error}")
        return uploaded_file_ids


def download_folder(service, dir_path, folder_id):
    files = list_files_in_folder(service, folder_id)
    for file in files:
        file_id = file["id"]
        file_name = file["name"]
        local_path = os.path.join(dir_path, file_name)
        download_file(service, file_id, local_path)
    print("Arquivos baixados com sucesso!")


if __name__ == "__main__":
    drive_service = get_drive_service()
    if not drive_service:
        print("Não foi possível autenticar na API do Drive.")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Faz o upload de arquivos de um diretório local para uma pasta do Drive."
    )

    parser.add_argument(
        "--mode",
        type=str,
        help="download || upload",
        required=True,
    )

    parser.add_argument(
        "--dir_path",
        type=str,
        help="Caminho para o diretório local (ex: './meus_arquivos')",
        required=True,
    )

    parser.add_argument(
        "--drive_folder_id",
        type=str,
        help="ID da pasta de destino no Drive",
        required=True,
    )

    args = parser.parse_args()

    dir_path = args.dir_path
    drive_folder_id = args.drive_folder_id
    mode = args.mode

    if mode == "upload":
        if not os.path.exists(dir_path):
            print(f"O diretório local '{dir_path}' nao foi encontrado.")
        else:
            ids_dos_arquivos_enviados = upload_multiple_files(
                drive_service,
                dir_path,
                drive_folder_id,
            )

            if ids_dos_arquivos_enviados:
                print("\nIDs acessados em tempo de execução:")
                print(ids_dos_arquivos_enviados)

    elif mode == "download":
        if not os.path.exists(dir_path):
            print(f"O diretório local '{dir_path}' nao foi encontrado.")
        else:
            download_folder(drive_service, dir_path, drive_folder_id)
