# 🚀 AI Student Assistant (RAG-based Gmail Assistant)

> Intelligent Gmail assistant powered by LLMs + Retrieval-Augmented Generation (RAG)

# Gmail Assistant

A powerful, RAG-enhanced email assistant that helps you manage, search, and analyze your Gmail inbox using local LLMs and Pathway.

## 🚀 Features

-   **Semantic Email Search**: Find emails by meaning, not just keywords, powered by Pathway's vector store.
-   **Real-Time Urgent Alerts**: Automatically attempts to identify and highlight urgent deadlines and tasks from your incoming emails.
-   **Interactive Dashboard**: Visual analytics for email traffic, top senders, and system status.
-   **Conversational Interface**: Chat naturally with your inbox. Ask "What did John say about the project last week?"
-   **Document Analysis**: Upload PDFs, CSVs, or Excel files for instant analysis and summarization.
-   **Privacy-First**: Runs locally with Ollama (Llama 3) for inference.

## 🛠️ Technology Stack

-   **Backend**: Python, Flask
-   **Data Processing & RAG**: Pathway
-   **LLM Inference**: Ollama (Llama 3)
-   **Frontend**: HTML5, TailwindCSS, React (embedded)
-   **Storage**: Local file system (simulated index)

## 📋 Prerequisites

1.  **Python 3.10+**
2.  **Ollama**: Installed and running (`ollama serve`).
3.  **Llama 3 Model**: Pulled via Ollama (`ollama run llama3.1`).
4.  **Google Credentials**: `credentials.json` for Gmail API access.

## ⚡ Quick Start

1.  **Install Dependencies**
    ```bash
    pip install flask flask-cors pathway requests google-auth-oauthlib google-api-python-client beautifulsoup4 pypdf pytesseract pillow pandas openpyxl
    ```

2.  **Start the RAG Backend (Pathway)**
    ```bash
    python app.py
    ```

3.  **Start the Web Server & Dashboard**
    Open a new terminal:
    ```bash
    python web_server.py
    ```

4.  **Access the Dashboard**
    Open your browser and navigate to:
    `http://localhost:5000`

## 🛡️ Architecture

-   `app.py`: Runs the Pathway data pipeline, indexing emails from `data/emails` and serving the RAG API.
-   `web_server.py`: Flask server that handles the frontend, file uploads, and acts as a gateway to Pathway and Ollama.
-   `gmail_watcher.py`: Fetches emails from Gmail and saves them to `data/emails` for indexing.
-   `static/index.html`: The single-page application dashboard.

## 🧠 Key Highlights
- RAG-based semantic search over emails
- Real-time email analysis using LLM (Llama 3 via Ollama)
- Interactive chatbot for inbox queries
- Privacy-first: runs locally

## 📝 License

Values-locked License.
