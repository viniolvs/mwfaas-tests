#!/bin/bash
# Script para configurar o ambiente, seja para desenvolvimento (Master) 
# ou para um Worker (Endpoint).
#
# Uso:
#   ./setup.sh                  (Configura o ambiente de desenvolvimento/Master)
#   ./setup.sh --install-endpoint (Configura um ambiente de Worker/Endpoint)

set -e # Sai imediatamente se um comando falhar

# clone submodule
git submodule update --init --recursive

# --- Parse do Argumento ---
INSTALL_ENDPOINT=0
if [[ "$1" == "--install-endpoint" ]]; then
    INSTALL_ENDPOINT=1
    echo "Modo de configuração de WORKER (Endpoint) ativado."
else
    echo "Modo de configuração de DESENVOLVIMENTO (Master) ativado."
fi

# --- Verificação de Dependências do Sistema (Python e Pip) ---
echo "--- Verificando Python 3 e Pip ---"
if ! command -v python3 &> /dev/null || ! command -v pip3 &> /dev/null; then
    echo "Python3 ou Pip3 não encontrados. Tentando instalar via apt (requer sudo)..."
    sudo apt-get update -q
    sudo apt-get install -y python3 python3-pip python3-venv
else
    echo "Python 3 e Pip 3 já estão instalados."
fi

# --- Criação do Ambiente Virtual (Venv) ---
VENV_DIR="venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "Criando ambiente virtual em ./${VENV_DIR}..."
    python3 -m venv "$VENV_DIR"
else
    echo "Ambiente virtual ./${VENV_DIR} já existe."
fi

# Define o caminho para o executável pip dentro do venv para evitar ter que 'source'
PIP_IN_VENV="$VENV_DIR/bin/pip3"

if [ $INSTALL_ENDPOINT -eq 1 ]; then
    echo "--- Instalando pacotes para o Worker/Endpoint ---"

    echo "Instalando globus-compute-endpoint..."
    "$PIP_IN_VENV" install globus-compute-endpoint

    WORKER_REQ_FILE="worker_requirements.txt"
    if [ -f "$WORKER_REQ_FILE" ]; then
        echo "Instalando dependências de $WORKER_REQ_FILE..."
        "$PIP_IN_VENV" install -r "$WORKER_REQ_FILE"
    else
        echo "Aviso: Arquivo '$WORKER_REQ_FILE' não encontrado."
        echo "Para que suas funções funcionem, crie este arquivo com as dependências do worker."
        echo "(Ex: google-api-python-client, numpy, pandas)"
    fi

    echo "--- Configuração do Worker concluída ---"
    echo "Para configurar o endpoint, ative o venv ($ source $VENV_DIR/bin/activate) e execute:"
    echo "globus-compute-endpoint configure SEU_NOME_DE_ENDPOINT"

else
    SUBMODULE_REQ_FILE="mwfaas/requirements.txt"
    if [ -f "$SUBMODULE_REQ_FILE" ]; then
        echo "Instalando dependências do submódulo $SUBMODULE_REQ_FILE..."
        "$PIP_IN_VENV" install -r "$SUBMODULE_REQ_FILE"
    else
        echo "Aviso: $SUBMODULE_REQ_FILE não encontrado."
        echo "Certifique-se de que o submódulo 'mwfaas' foi baixado (git submodule update --init --recursive)"
    fi
    echo "--- Configuração de Desenvolvimento concluída ---"
    echo "Para ativar o ambiente, execute: source $VENV_DIR/bin/activate"
fi

echo "Script finalizado."
