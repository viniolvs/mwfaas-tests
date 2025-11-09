package main

import (
	"bufio"
	"flag"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {
	mbPtr := flag.Int("mb", 10, "Tamanho alvo do arquivo em Megabytes (MB)")
	outputPtr := flag.String("o", "dataset.json", "Nome do arquivo de saída JSON")
	flag.Parse()

	targetSizeMB := *mbPtr
	targetSizeBytes := int64(targetSizeMB) * 1024 * 1024
	outputFile := *outputPtr

	log.Printf("Iniciando geração de '%s' com tamanho alvo de %d MB...\n", outputFile, targetSizeMB)

	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Falha ao criar o arquivo: %v\n", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	const maxRandomValue = 100001

	var currentSize int64 = 0

	n, _ := writer.WriteString("[")
	currentSize += int64(n)

	firstItem := true
	for currentSize < targetSizeBytes {
		// --- LÓGICA DE ESCRITA CORRIGIDA ---

		// 1. Gera o número e seu tamanho
		num := r.Intn(maxRandomValue)
		numStr := strconv.Itoa(num)
		numSize := int64(len(numStr))

		// 2. Calcula o tamanho que vamos adicionar (número + vírgula, se necessário)
		bytesToAdd := numSize
		if !firstItem {
			bytesToAdd += 1 // Adiciona 1 byte para a vírgula
		}

		// 3. Verifica se o que vamos adicionar + o colchete final cabem
		//    (Adiciona +1 para o ']' de fechamento)
		if (currentSize + bytesToAdd + 1) > targetSizeBytes {
			// Não há espaço suficiente para este número E o colchete final.
			// Paramos o laço ANTES de escrever a vírgula.
			break
		}

		// 4. Se houver espaço, escreve a vírgula (se não for o primeiro item)
		if !firstItem {
			n, _ = writer.WriteString(",")
			currentSize += int64(n)
		} else {
			firstItem = false
		}

		// 5. Escreve o número
		n, _ = writer.WriteString(numStr)
		currentSize += int64(n)

		// --- FIM DA LÓGICA CORRIGIDA ---
	}

	// Escreve o colchete de fechamento final
	writer.WriteString("]")
	writer.Flush() // Garante que tudo seja escrito

	info, _ := file.Stat()
	finalSizeMB := float64(info.Size()) / (1024 * 1024)
	log.Printf("Concluído. Tamanho final do arquivo: %.2f MB\n", finalSizeMB)
}
