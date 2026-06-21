#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "🚀 [Step 1/4] Downloading and installing Ollama..."
curl -fsSL https://ollama.com/install.sh | sh

echo "⚙️ [Step 2/4] Starting Ollama daemon in the background..."
# We use nohup to run the server in the background and route logs to a file
nohup ollama serve > ollama_server.log 2>&1 &

echo "⏳ [Step 3/4] Waiting for the Ollama API to initialize..."
# Give the daemon 5 seconds to bind to port 11434
sleep 5

echo "📥 [Step 4/4] Pulling the Llama 3.1 model (This will take a few minutes)..."
ollama pull llama3.1

echo "================================================================"
echo "✅ SUCCESS: Local Air-Gapped AI Environment is ready!"
echo "📄 Server logs are being written to: ollama_server.log"
echo "💻 You can now execute: python agent.py"
echo "================================================================"