#!/bin/bash

# set -e  #  para o script se uma execução falhar

which python3

SCRIPT_NAME="mandelbrot.py"
WIDTH=3000
HEIGHT=2000
MAX_ITER=4000
LINES_PER_WORKER=100

NUM_EXECUTIONS=5

OUTPUT_DIR="outputs/mandelbrot/${HEIGHT}_${LINES_PER_WORKER}"
mkdir -p "$OUTPUT_DIR"

echo "Iniciando $NUM_EXECUTIONS execuções..."
echo "Salvando logs em $OUTPUT_DIR/"

for (( i=1; i<=$NUM_EXECUTIONS; i++ ))
do
    LOG_FILE="${OUTPUT_DIR}/${i}_${WIDTH}x${HEIGHT}_${MAX_ITER}.txt"
    OUTPUT_FILE="${OUTPUT_DIR}/${i}_${WIDTH}x${HEIGHT}_${MAX_ITER}.png"
    echo "--- Execução $i de $NUM_EXECUTIONS ---"
    python3 "$SCRIPT_NAME" "$OUTPUT_FILE"  --width $WIDTH --height $HEIGHT --iter $MAX_ITER --lines $LINES_PER_WORKER &> "$LOG_FILE" 2>&1
    echo "Log da Execução $i salvo em $LOG_FILE"
done

echo "--- Todas as execuções foram concluídas. ---"
