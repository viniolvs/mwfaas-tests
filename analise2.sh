#!/bin/bash
# Script para Contagem de Tarefas por Worker
# Recebe o nome do arquivo como argumento da linha de comando.
#
# Exemplo de uso:
# ./contar_workers.sh gzip_1.txt
#

# --- Validação do Argumento ---
# Verifica se um argumento (nome do arquivo) foi fornecido
if [ -z "$1" ]; then
    echo "Erro: Você precisa fornecer o nome do arquivo como argumento." >&2
    echo "Exemplo de uso: $0 nome_do_arquivo.txt" >&2
    exit 1
fi

# Usa o primeiro argumento da linha de comando ($1) como o nome do arquivo
FILENAME=$1

# Verifica se o arquivo existe
if [ ! -f "$FILENAME" ]; then
    echo "Erro: Arquivo '$FILENAME' não encontrado." >&2
    exit 1
fi

echo "Analisando o arquivo: $FILENAME"

echo "-------------------------------------"

echo "[Master] $(grep -E "master.run()" $FILENAME)"
echo "$(grep -E "Master" $FILENAME | grep -v -E "Execution times")"

# --- Execução da Contagem ---
echo "-------------------------------------"
echo "Tarefas executadas por Worker lab_1, lab_2, desktop, laptop_1, laptop_2: "
echo "$(grep -c -E "c5d1ef1f-76c0-40e0-a9b0-e1cfbd95ecd8.+completou" $FILENAME)" # lab_1
echo "$(grep -c -E "d93b465f-d252-4574-8946-9ea53e5cdd6e.+completou" $FILENAME)" # lab_2
echo "$(grep -c -E "af3264fb-0cf3-49b2-b104-68863dec7cb8.+completou" $FILENAME)" # desktop
echo "$(grep -c -E "9f08856d-404c-4775-a23a-6c7afa5c768e.+completou" $FILENAME)" # laptop_1
echo "$(grep -c -E "d2652e35-4b34-4496-9e0e-d51b65b7d194.+completou" $FILENAME)" # laptop_2
