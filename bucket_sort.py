import argparse
import json
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import cloudpickle

from mwfaas.globus_compute_manager import GlobusComputeCloudManager
from mwfaas.list_distribuition_strategy import ListDistributionStrategy
from mwfaas.master import Master


def sort_bucket_worker(
    chunk: List[List[int]], metadata: Dict[str, Any]
) -> Dict[str, Any]:
    import time

    try:
        start_time = time.perf_counter()
        bucket_to_sort = chunk[0]
        bucket_to_sort.sort()
        end_time = time.perf_counter()
        return {"time": end_time - start_time, "data": bucket_to_sort}

    except Exception as e:
        print(f"[Worker] Erro ao tentar ordenar o chunk: {e}")
        raise e


def distribute_into_buckets_local(
    data: List[int], num_buckets: int, max_value: int
) -> List[List[int]]:
    print(
        f"[Master] Distribuindo {len(data)} itens em {num_buckets} baldes localmente..."
    )
    bucket_size = (max_value / num_buckets) + 1e-9
    local_buckets = defaultdict(list)
    for number in data:
        bucket_index = int(number // bucket_size)
        if bucket_index >= num_buckets:
            bucket_index = num_buckets - 1
        local_buckets[bucket_index].append(number)
    bucket_list = [local_buckets[i] for i in range(num_buckets)]
    print("[Master] Distribuição local concluída.")
    return bucket_list


def prepare_data(json_filepath: str, num_buckets: int) -> Tuple[List[List[int]], int]:
    print(f"[Master] Lendo dados de entrada do arquivo: {json_filepath}...")
    print(f"[Master] Usando {num_buckets} baldes para a distribuição.")

    try:
        with open(json_filepath, "r") as f:
            full_data_list = json.load(f)

        if not isinstance(full_data_list, list):
            raise TypeError("O arquivo JSON não contém uma lista (array) na raiz.")

        if not full_data_list:
            print("Arquivo JSON está vazio. Encerrando.")
            sys.exit(0)

        NUM_ITENS = len(full_data_list)
        MAX_VALUE = max(full_data_list)
        NUM_BUCKETS = num_buckets

        print(f"Dados lidos: {NUM_ITENS:,} itens, valor máximo encontrado: {MAX_VALUE}")
        print(f"Configurando para {NUM_BUCKETS} baldes.")

    except FileNotFoundError:
        print(f"ERRO: Arquivo JSON não encontrado em '{json_filepath}'")
        sys.exit(1)
    except json.JSONDecodeError:
        print(
            f"ERRO: Falha ao decodificar. O arquivo '{json_filepath}' não é um JSON válido."
        )
        sys.exit(1)
    except (TypeError, ValueError) as e:
        print(f"ERRO: Os dados no JSON não são uma lista de números. Detalhe: {e}")
        sys.exit(1)

    unsorted_buckets = distribute_into_buckets_local(
        full_data_list, NUM_BUCKETS, MAX_VALUE
    )

    print("\n[Master] Analisando e filtrando baldes para envio...")
    tasks_to_run: List[List[int]] = []
    total_payload_mb = 0

    for i, bucket in enumerate(unsorted_buckets):
        if bucket:
            serialized_bucket = cloudpickle.dumps(bucket)
            size_in_bytes = sys.getsizeof(serialized_bucket)
            size_in_mb = size_in_bytes / (1024 * 1024)
            print(
                f"  - Balde {i}: {len(bucket):,} itens, Tamanho (Payload): {size_in_mb:.2f} MB"
            )
            tasks_to_run.append(bucket)
            total_payload_mb += size_in_mb

        else:
            print(f"  - Balde {i}: Vazio (ignorado).")

    print(
        f"\n[Master] {len(tasks_to_run)} baldes não-vazios serão enviados para ordenação."
    )
    print(f"[Master] Carga de trabalho total (Payload): {total_payload_mb:.2f} MB")
    return tasks_to_run, len(full_data_list)


def main(json_filepath: str, num_buckets: int):
    """
    Função principal que agora recebe os argumentos validados.
    """

    tasks_to_run, num_items = prepare_data(json_filepath, num_buckets)

    with GlobusComputeCloudManager() as cloud_manager:
        strategy = ListDistributionStrategy(items_per_chunk=1)
        master = Master(cloud_manager, distribution_strategy=strategy)

        start_time = time.perf_counter()
        sorted_buckets_results = master.run(
            data_input=tasks_to_run,
            user_function=sort_bucket_worker,
            metadata=None,
        )
        end_time = time.perf_counter()

        print("\n--- FASE DE AGREGAÇÃO (Concatenando resultados no Master) ---")
        execution_times = []
        sorted_buckets = []
        for result in sorted_buckets_results:
            if isinstance(result, dict):
                sorted_buckets.append(result.get("data", []))
                execution_times.append(result.get("time", 0))

        final_sorted_list = []
        for bucket in sorted_buckets:
            final_sorted_list.extend(bucket)

        print(f"Tempo de execução master.run(): {end_time - start_time:.4f} segundos")
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

        if len(final_sorted_list) == num_items:
            print("VERIFICAÇÃO: Sucesso! O tamanho da lista final bate com a original.")
        else:
            print(
                "VERIFICAÇÃO: FALHA! O tamanho da lista final é diferente da original."
            )


def main_local(json_filepath: str, num_buckets: int):
    tasks_to_run, num_items = prepare_data(json_filepath, num_buckets)
    results = []

    start_time = time.perf_counter()
    for bucket in tasks_to_run:
        results.append(sort_bucket_worker([bucket], {}))
    end_time = time.perf_counter()

    sorted_buckets = []
    execution_times = []
    for result in results:
        if isinstance(result, dict):
            sorted_buckets.append(result.get("data", []))
            execution_times.append(result.get("time", 0))

    final_sorted_list = []
    for bucket in sorted_buckets:
        final_sorted_list.extend(bucket)

    if len(final_sorted_list) == num_items:
        print("VERIFICAÇÃO: Sucesso! O tamanho da lista final bate com a original.")
    else:
        print("VERIFICAÇÃO: FALHA! O tamanho da lista final é diferente da original.")

    print(f"[Local] Tempo de execução total: {end_time - start_time:.4f} segundos")
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Processa um arquivo JSON e o divide em buckets."
    )

    parser.add_argument(
        "json_filepath",
        type=str,
        help="Caminho para o arquivo .json de entrada (obrigatório)",
    )

    parser.add_argument(
        "num_buckets",
        type=int,
        nargs="?",
        default=100,
        help="Número de buckets para dividir (opcional, padrão: 100)",
    )

    parser.add_argument(
        "--run_local",
        action="store_true",
        help="Se presente, executa o script localmente",
    )

    args = parser.parse_args()
    if args.num_buckets <= 0:
        print(
            f"Erro: O número de buckets ({args.num_buckets}) deve ser um inteiro positivo."
        )
        parser.print_usage()
        sys.exit(1)

    if args.run_local:
        main_local(args.json_filepath, args.num_buckets)
    else:
        main(args.json_filepath, args.num_buckets)
