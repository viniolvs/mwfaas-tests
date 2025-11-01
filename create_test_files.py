import argparse
import os
import sys

ONE_MEGABYTE = 1024 * 1024

CHUNK_SIZE = ONE_MEGABYTE


def create_dummy_file(full_filepath, size_mb):
    """
    Cria um arquivo de texto com um tamanho (aproximado) em Megabytes.

    Args:
        full_filepath (str): O caminho completo (incluindo diretório) do arquivo.
        size_mb (float): O tamanho desejado em MB.
    """
    try:
        total_size_bytes = int(size_mb * ONE_MEGABYTE)

        # Pega apenas o nome do arquivo para o log
        filename = os.path.basename(full_filepath)
        print(f"Criando '{filename}' (Tamanho: {size_mb} MB)...")

        chunk_data = "0" * CHUNK_SIZE
        num_chunks = total_size_bytes // CHUNK_SIZE
        remainder = total_size_bytes % CHUNK_SIZE

        with open(full_filepath, "w", encoding="utf-8") as f:
            for _ in range(num_chunks):
                f.write(chunk_data)

            if remainder > 0:
                f.write("0" * remainder)

        final_size = os.path.getsize(full_filepath)
        print(
            f" -> Concluído: '{full_filepath}' (Tamanho real: {(final_size / ONE_MEGABYTE):.2f} MB)"
        )

    except IOError as e:
        print(
            f"Erro de E/S ao escrever o arquivo '{full_filepath}': {e}", file=sys.stderr
        )
    except Exception as e:
        print(
            f"Um erro inesperado ocorreu durante a criação de '{full_filepath}': {e}",
            file=sys.stderr,
        )


def get_output_filename(base_name, index):
    """
    Gera o nome do arquivo sequencial.
    Ex: ('arquivo.txt', 1) -> 'arquivo_1.txt'
    """
    base, ext = os.path.splitext(base_name)
    return f"{base}_{index}{ext}"


def ensure_directory_exists(directory_path):
    """
    Verifica se um diretório existe e, se não, o cria.
    """
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
            print(f"Diretório '{directory_path}' criado.")
        except OSError as e:
            print(f"Erro ao criar o diretório '{directory_path}': {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cria N arquivos de texto 'dummy' em um diretório específico."
    )

    parser.add_argument(
        "size_mb",
        type=float,
        help="Tamanho desejado para CADA arquivo em megabytes. Ex: 10 ou 2.5",
    )

    parser.add_argument(
        "-n",
        "--numero",
        type=int,
        default=1,
        help="Número (N) de arquivos a serem criados (padrão: 1)",
    )

    parser.add_argument(
        "-o",
        "--output",
        default="dummy_file.txt",
        help="Nome base do arquivo de saída (padrão: dummy_file.txt). Um índice será adicionado.",
    )

    parser.add_argument(
        "-d",
        "--directory",
        default=".",
        help="Diretório de destino para salvar os arquivos (padrão: diretório atual)",
    )

    args = parser.parse_args()

    if args.numero <= 0:
        print("Erro: O número de arquivos deve ser 1 ou maior.", file=sys.stderr)
        sys.exit(1)

    ensure_directory_exists(args.directory)

    print(f"\nIniciando criação de {args.numero} arquivo(s)...")
    print(f"Diretório de destino: {args.directory}")
    print(f"Tamanho por arquivo: {args.size_mb} MB")
    print(f"Nome base: {args.output}\n")

    for i in range(1, args.numero + 1):
        filename = get_output_filename(args.output, i)
        full_output_path = os.path.join(args.directory, filename)
        create_dummy_file(full_output_path, args.size_mb)

    print(
        f"\nOperação concluída. {args.numero} arquivo(s) criado(s) em '{args.directory}'."
    )
