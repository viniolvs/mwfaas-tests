from globus_compute_sdk import Executor, ShellFunction

ENDPOINT_ID = "a4c2d8a0-2ac2-4758-a5a3-93751ec1aac7"

with Executor(endpoint_id=ENDPOINT_ID) as gce:
    run_go_program = ShellFunction("echo  'Hello World!'")

    # 2. Submeta o comando ao endpoint
    print("Submetendo a tarefa (programa Go) ao endpoint...")
    future = gce.submit(run_go_program)

    # 3. Aguarde e obtenha o resultado
    result = future.result()

    # O resultado é um objeto ShellResult
    print("Tarefa concluída.")
    print(f"Comando que foi executado: {result.cmd}")
    print(f"Código de Retorno: {result.returncode}")
    print("\n--- Saída (stdout) ---")
    print(result.stdout)
    print("------------------------")
    print("\n--- Erro (stderr) ----")
    print(result.stderr)
    print("------------------------")
