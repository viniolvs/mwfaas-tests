#!/bin/bash

# set -e  #  para o script se uma execução falhar

which python3

SCRIPT_NAME="bucket_sort.py"
BUCKET_COUNT=50
OUTPUT_DIR="outputs/bucket_sort/local/buckets_$BUCKET_COUNT"
NUM_EXECUTIONS=5
INPUT_FILE="inputs/small_dataset.json"


mkdir -p "$OUTPUT_DIR"

echo "Iniciando $NUM_EXECUTIONS execuções..."
echo "Salvando logs em $OUTPUT_DIR/"

for (( i=1; i<=$NUM_EXECUTIONS; i++ ))
do
    LOG_FILE="${OUTPUT_DIR}/${i}.txt"
    echo "--- Execução $i de $NUM_EXECUTIONS ---"
    python3 "$SCRIPT_NAME" "$INPUT_FILE" "$BUCKET_COUNT" --run_local > "$LOG_FILE" 2>&1
    echo "Log da Execução $i salvo em $LOG_FILE"
done

echo "--- Todas as execuções foram concluídas. ---"
