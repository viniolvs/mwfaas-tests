#!/bin/bash

# set -e  #  para o script se uma execução falhar

which python3

# O nome do seu script Python
SCRIPT_NAME="globus_gzip_google_drive.py"

INPUT_FOLDER_ID="1E028ele9aznuqNqQE8eiixm6HDE2iyo5"
OUTPUT_FOLDER_ID="1Cg626mGKxYdkwYGx0cKOSK4DcznpE2V6"

# O diretório base para os arquivos de log
OUTPUT_DIR="outputs/gzip/many_per_worker"

# O número de vezes que você quer executar o comando
NUM_EXECUTIONS=5

# --- 2. Preparação ---

mkdir -p "$OUTPUT_DIR"

echo "Iniciando $NUM_EXECUTIONS execuções..."
echo "Salvando logs em $OUTPUT_DIR/"

for (( i=1; i<=$NUM_EXECUTIONS; i++ ))
do
    LOG_FILE="${OUTPUT_DIR}/${i}.txt"
    echo "--- Execução $i de $NUM_EXECUTIONS ---"
    python3 "$SCRIPT_NAME" "$INPUT_FOLDER_ID" "$OUTPUT_FOLDER_ID" > "$LOG_FILE" 2>&1
    echo "Log da Execução $i salvo em $LOG_FILE"
done

echo "--- Todas as execuções foram concluídas. ---"
