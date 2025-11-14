import argparse
import sys
import time
from typing import Any, Dict, List

from mwfaas.globus_compute_manager import GlobusComputeCloudManager
from mwfaas.list_distribuition_strategy import ListDistributionStrategy
from mwfaas.master import Master

try:
    from PIL import Image
except ImportError:
    print("Erro: A biblioteca Pillow é necessária para este exemplo.")
    print("Por favor, instale-a no venv do Master com: pip install Pillow")
    sys.exit(1)


def mandelbrot_worker(chunk: List[int], metadata: Dict[str, Any]) -> Dict[str, Any]:
    # ) -> List[Tuple[int, List[int]]]:
    """
    Função do Worker.
    Calcula um LOTE de linhas do conjunto de Mandelbrot.
    O chunk é uma lista de números de linha, ex: [10, 11, 12, ..., 19].
    """

    import time

    start_time = time.perf_counter()

    WIDTH = metadata.get("width", 800)
    HEIGHT = metadata.get("height", 600)
    X_MIN = metadata.get("x_min", -2.0)
    X_MAX = metadata.get("x_max", 1.0)
    Y_MIN = metadata.get("y_min", -1.0)
    Y_MAX = metadata.get("y_max", 1.0)
    MAX_ITER = metadata.get("max_iter", 255)

    results = []
    try:
        for y_pixel in chunk:
            # Converte a coordenada do pixel Y para a coordenada do plano complexo
            y0 = Y_MIN + (y_pixel / HEIGHT) * (Y_MAX - Y_MIN)

            row_colors = []

            # Itera sobre cada pixel X nesta linha
            for x_pixel in range(WIDTH):
                x0 = X_MIN + (x_pixel / WIDTH) * (X_MAX - X_MIN)

                # (Cálculo do Mandelbrot)
                c = complex(x0, y0)
                z = 0 + 0j
                iteration = 0
                while abs(z) <= 2 and iteration < MAX_ITER:
                    z = z * z + c
                    iteration += 1

                row_colors.append(iteration)

            results.append((y_pixel, row_colors))

        end_time = time.perf_counter()
        return {
            "data": results,
            "time": end_time - start_time,
            "chunk_avg_time": (end_time - start_time) / len(results),
        }

    except Exception as e:
        print(
            f"[Worker] Erro ao processar o chunk de linhas {chunk[0]}...{chunk[-1]}: {e}"
        )
        raise e


def main(
    output_filename: str,
    image_width: int,
    image_height: int,
    max_iterations: int,
    lines_per_worker: int,
):
    IMAGE_WIDTH = image_width
    IMAGE_HEIGHT = image_height
    MAX_ITERATIONS = max_iterations
    LINES_PER_TASK = lines_per_worker

    total_tasks = (IMAGE_HEIGHT + LINES_PER_TASK - 1) // LINES_PER_TASK

    print(f"Iniciando renderização de Mandelbrot ({IMAGE_WIDTH}x{IMAGE_HEIGHT})...")
    print(f"Agrupando {IMAGE_HEIGHT} linhas em lotes de {LINES_PER_TASK}.")
    print(f"Total de {total_tasks} tarefas paralelas a serem submetidas.")

    tasks_to_run = list(range(IMAGE_HEIGHT))

    task_metadata = {
        "width": IMAGE_WIDTH,
        "height": IMAGE_HEIGHT,
        "max_iter": MAX_ITERATIONS,
        "x_min": -2.0,
        "x_max": 1.0,
        "y_min": -1.0,
        "y_max": 1.0,
    }

    with GlobusComputeCloudManager() as cloud_manager:
        strategy = ListDistributionStrategy(items_per_chunk=LINES_PER_TASK)
        master = Master(cloud_manager, distribution_strategy=strategy)

        start_time = time.perf_counter()

        results = master.run(
            data_input=tasks_to_run,
            user_function=mandelbrot_worker,
            metadata=task_metadata,
        )

        end_time = time.perf_counter()
        print(f"Tempo de execução master.run(): {end_time - start_time:.4f} segundos")

        print("\n--- FASE DE AGREGAÇÃO (Construindo imagem final no Master) ---")

        successful_chunks = []
        execution_times = []
        chunk_avg_times = []
        for r in results:
            if not isinstance(r, dict):
                print(f"ERRO: Resultado não reconhecido: {r}")
                continue
            if "data" in r:
                successful_chunks.append(r["data"])
            if "time" in r:
                execution_times.append(r["time"])
            if "chunk_avg_time" in r:
                chunk_avg_times.append(r["chunk_avg_time"])

        successful_rows = []
        for chunk_result in successful_chunks:
            successful_rows.extend(chunk_result)

        if not successful_rows:
            print(
                "Nenhuma tarefa foi concluída com sucesso. Imagem não pode ser gerada."
            )
            return

        successful_rows.sort(key=lambda x: x[0])

        img = Image.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), color="black")
        pixels = img.load()
        if not pixels:
            print("Erro: A imagem não pode ser gerada.")
            return

        for y, row_data in successful_rows:
            for x, iterations in enumerate(row_data):
                color_value = 255 - (iterations % 256)
                pixels[x, y] = (color_value, color_value, color_value)

        img.save(output_filename)
        print(f"\nImagem salva com sucesso em '{output_filename}'")
        print(f"Total de {len(successful_rows)} linhas renderizadas.")

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

        if chunk_avg_times:
            print(
                f"\n[Master] Tempo médio de execução por chunk: {sum(chunk_avg_times) / len(chunk_avg_times):.4f}s"
            )
            print(
                f"[Master] Tempo máximo de execução de um chunk: {max(chunk_avg_times):.4f}s"
            )
            print(
                f"[Master] Tempo mínimo de execução de um chunk: {min(chunk_avg_times):.4f}s"
            )
            print("[Master] Chunk average times:", chunk_avg_times)

        print("\n" + "-" * 15 + " Status das Tarefas " + "-" * 15)
        print(master.get_task_statuses())


def main_local(
    output_filename: str,
    image_width: int,
    image_height: int,
    max_iterations: int,
    lines_per_worker: int,
):
    """
    Executa o cálculo do Mandelbrot localmente, sem o Globus Compute.
    """
    IMAGE_WIDTH = image_width
    IMAGE_HEIGHT = image_height
    MAX_ITERATIONS = max_iterations
    LINES_PER_TASK = lines_per_worker

    total_tasks = (IMAGE_HEIGHT + LINES_PER_TASK - 1) // LINES_PER_TASK

    print(
        f"[Local] Iniciando renderização de Mandelbrot ({IMAGE_WIDTH}x{IMAGE_HEIGHT})..."
    )
    print(f"[Local] Agrupando {IMAGE_HEIGHT} linhas em lotes de {LINES_PER_TASK}.")
    print(f"[Local] Total de {total_tasks} tarefas locais a serem executadas.")

    tasks_to_run = list(range(IMAGE_HEIGHT))

    task_metadata = {
        "width": IMAGE_WIDTH,
        "height": IMAGE_HEIGHT,
        "max_iter": MAX_ITERATIONS,
        "x_min": -2.0,
        "x_max": 1.0,
        "y_min": -1.0,
        "y_max": 1.0,
    }

    print("[Local] Iniciando processamento local...")
    start_time = time.perf_counter()

    results = []
    # Itera manualmente sobre os chunks (lotes) de tarefas
    for i in range(0, IMAGE_HEIGHT, LINES_PER_TASK):
        # Cria o chunk de linhas (ex: [0, 1, ..., 9])
        chunk = tasks_to_run[i : i + LINES_PER_TASK]
        if not chunk:
            continue

        try:
            worker_result = mandelbrot_worker(chunk, task_metadata)
            results.append(worker_result)
        except Exception as e:
            print(
                f"[Local] Erro ao processar o chunk localmente {chunk[0]}...{chunk[-1]}: {e}"
            )

    end_time = time.perf_counter()
    print(f"[Local] Tempo de execução local: {end_time - start_time:.4f} segundos")
    # --- Fim do Bloco de Execução Local ---

    print("\n--- FASE DE AGREGAÇÃO (Construindo imagem final) ---")

    successful_chunks = []
    execution_times = []
    chunk_avg_times = []
    for r in results:
        if not isinstance(r, dict):
            print(f"ERRO: Resultado não reconhecido: {r}")
            continue
        if "data" in r:
            successful_chunks.append(r["data"])
        if "time" in r:
            execution_times.append(r["time"])
        if "chunk_avg_time" in r:
            chunk_avg_times.append(r["chunk_avg_time"])

    successful_rows = []
    for chunk_result in successful_chunks:
        successful_rows.extend(chunk_result)

    if not successful_rows:
        print("Nenhuma tarefa foi concluída com sucesso. Imagem não pode ser gerada.")
        return

    successful_rows.sort(key=lambda x: x[0])

    img = Image.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), color="black")
    pixels = img.load()
    if not pixels:
        print("Erro: A imagem não pode ser gerada.")
        return

    for y, row_data in successful_rows:
        for x, iterations in enumerate(row_data):
            color_value = 255 - (iterations % 256)
            pixels[x, y] = (color_value, color_value, color_value)

    img.save(output_filename)
    print(f"\nImagem salva com sucesso em '{output_filename}'")
    print(f"Total de {len(successful_rows)} linhas renderizadas.")

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

    if chunk_avg_times:
        print(
            f"\n[Local] Tempo médio de execução por chunk: {sum(chunk_avg_times) / len(chunk_avg_times):.4f}s"
        )
        print(
            f"[Local] Tempo máximo de execução de um chunk: {max(chunk_avg_times):.4f}s"
        )
        print(
            f"[Local] Tempo mínimo de execução de um chunk: {min(chunk_avg_times):.4f}s"
        )
        print("[Local] Chunk average times:", chunk_avg_times)

    print("\n[Local] Execução local concluída.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Executa o framework mwfaas para renderizar um fractal Mandelbrot.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "output_filename",
        type=str,
        help="Nome do arquivo de imagem PNG de saída (ex: mandelbrot.png)",
    )

    # Argumentos Opcionais (com --)
    parser.add_argument(
        "--width", type=int, default=1200, help="Largura da imagem em pixels."
    )
    parser.add_argument(
        "--height", type=int, default=800, help="Altura da imagem em pixels."
    )
    parser.add_argument(
        "--iter", type=int, default=255, help="Número máximo de iterações por pixel."
    )
    parser.add_argument(
        "--lines",
        type=int,
        default=10,
        help="Número de linhas de pixel a serem agrupadas em cada tarefa (chunk).",
    )

    parser.add_argument(
        "--run_local",
        action="store_true",
        help="Se presente, executa o script localmente",
    )

    args = parser.parse_args()

    try:
        if args.run_local:
            main_local(
                output_filename=args.output_filename,
                image_width=args.width,
                image_height=args.height,
                max_iterations=args.iter,
                lines_per_worker=args.lines,
            )
        else:
            main(
                output_filename=args.output_filename,
                image_width=args.width,
                image_height=args.height,
                max_iterations=args.iter,
                lines_per_worker=args.lines,
            )
    except Exception as e:
        print("\nERRO: Uma falha inesperada ocorreu durante a execução:")
        print(f"{type(e).__name__}: {e}")
        sys.exit(1)
