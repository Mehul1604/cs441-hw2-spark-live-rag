#!/bin/bash
# emr-bootstrap.sh: EMR bootstrap action for Delta Rag Indexer

# Enable logging and exit on error
set -x
set -e

echo "🚀 Starting Incremental Rag Indexer EMR Bootstrap"

# --- Set Environment Variables for RAG Application ---
echo "--- Setting up environment variables ---"
# Set APP_ENV to emr so the application uses EMR-specific configuration
export APP_ENV=emr
echo "export APP_ENV=emr" | sudo tee -a /etc/environment
echo "export APP_ENV=emr" | sudo tee -a /home/hadoop/.bashrc
echo "export APP_ENV=emr" | sudo tee -a /home/ec2-user/.bashrc

# Also set it in the systemd environment for services
sudo mkdir -p /etc/systemd/system.conf.d
echo "[Manager]" | sudo tee /etc/systemd/system.conf.d/app-env.conf
echo "DefaultEnvironment=APP_ENV=emr" | sudo tee -a /etc/systemd/system.conf.d/app-env.conf

echo "✅ APP_ENV set to 'emr' for all users and services"

# --- Install Java 17 (Amazon Corretto) ---
echo "--- Installing Amazon Corretto 17 ---"
sudo yum install -y java-17-amazon-corretto-devel

# --- Verify Java Installation ---
echo "--- Verifying Java installation ---"
if command -v java >/dev/null 2>&1; then
    echo "Java is installed. Version information:"
    java -version
else
    echo "Error: Java is not installed."
    exit 1
fi

# --- Verify Spark Installation ---
echo "--- Verifying Spark installation ---"
if command -v spark-submit >/dev/null 2>&1; then
    echo "Spark is installed. Version information:"
    spark-submit --version
    echo "✅ spark-submit command is available and runnable"
else
    echo "Error: Spark is not installed or spark-submit is not available."
    exit 1
fi

# Test spark-submit with a simple command
echo "--- Testing spark-submit functionality ---"
if spark-submit --help >/dev/null 2>&1; then
    echo "✅ spark-submit help command executed successfully"
else
    echo "⚠️ Warning: spark-submit help command failed, but continuing..."
fi

# --- Verify Hadoop Installation ---
# Note: Hadoop 3.4.1 is installed by the EMR creation process, not the bootstrap script.
# This part of the script simply checks if it's available and working.
echo "--- Verifying Hadoop installation ---"
if command -v hadoop >/dev/null 2>&1; then
    echo "Hadoop is installed. Version information:"
    hadoop version
else
    echo "Error: Hadoop is not installed."
    exit 1
fi

# --- Check HDFS file system (optional but recommended) ---
echo "--- Verifying HDFS file system ---"
# This is a good way to test if the Hadoop services are running correctly.
# It uses the `hdfs` command to list the root directory.
sudo -u hadoop hdfs dfs -ls /

# Install Ollama
echo "🤖 Installing Ollama..."
curl -fsSL https://ollama.com/install.sh | sh

# Create ollama service user if it doesn't exist
sudo useradd -r -s /bin/false -m -d /usr/share/ollama ollama || true

# Create ollama directories
sudo mkdir -p /etc/systemd/system/ollama.service.d
sudo mkdir -p /var/lib/ollama
sudo chown ollama:ollama /var/lib/ollama

# Configure Ollama systemd service to bind to all interfaces
sudo tee /etc/systemd/system/ollama.service.d/override.conf > /dev/null <<'EOF'
[Service]
Environment="OLLAMA_HOST=0.0.0.0:11434"
Environment="OLLAMA_ORIGINS=*"
Environment="OLLAMA_KEEP_ALIVE=10m"
Environment="OLLAMA_NUM_PARALLEL=1"
EOF

# Start and enable Ollama service
sudo systemctl daemon-reload
sudo systemctl enable ollama
sudo systemctl start ollama

# Wait for Ollama to be ready
echo "⏳ Waiting for Ollama to start..."
for i in {1..30}; do
    if curl -s http://localhost:11434/api/tags > /dev/null; then
        echo "✅ Ollama is ready!"
        break
    fi
    echo "Waiting for Ollama... ($i/30)"
    sleep 2
done

# Pull required models
echo "📥 Pulling required Ollama models..."
sudo -u ollama ollama pull mxbai-embed-large &
wait

echo "✅ Ollama models pulled successfully"

# Add warmup section
echo "🔥 Warming up Ollama models on this node..."

# Wait a bit more for models to be fully loaded
sleep 10


# Warmup embedding model
echo "🔍 Warming up mxbai-embed-large model..."
sudo -u ollama curl -X POST http://localhost:11434/api/embeddings \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mxbai-embed-large",
    "prompt": "This is a warmup test for embeddings to ensure the model is loaded in memory."
  }' \
  --max-time 120 --silent || echo "⚠️ Embedding warmup timeout, continuing..."

echo "🎯 Ollama warmup completed on node $(hostname)"

# --- List the contents of your S3 bucket (for troubleshooting) ---
echo "--- Listing S3 bucket contents ---"
aws s3 ls s3://cs441-incremental-delta-rag/
