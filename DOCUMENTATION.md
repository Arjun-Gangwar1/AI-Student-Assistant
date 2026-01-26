# Gmail Assistant Documentation

## Core Features

### 1. Semantic Search (RAG)
The core of the assistant is the Retrieval-Augmented Generation (RAG) engine built with Pathway.
-   **Mechanism**: Emails are converted into vector embeddings. When you ask a question, the system retrieves the most relevant email chunks and feeds them to the LLM.
-   **Benefit**: Allows for natural language queries like "Find the invoice from last Tuesday" even if exact keywords don't match.

### 2. Urgent Alerts
The system proactively scans retrieved contexts for urgent keywords ("ASAP", "Deadline", "Urgent").
-   **Workflow**:
    1.  Frontend requests `/api/alerts`.
    2.  Backend performs a broad search for urgent terms.
    3.  Top results are sent to the LLM with a specific prompt to extract structured task data.
    4.  Tasks are displayed in the "URGENT.ALERTS" widget.

### 3. Dashboard Analytics
Real-time visualization of your inbox data.
-   **Traffic Volume**: Displays email volume over the last 7 days.
-   **Top Senders**: Identifies who emails you the most.
-   **Process**: Computed on-the-fly by parsing the raw email files in `data/emails`.

### 4. Document Analysis
Upload and chat with external documents.
-   **Supported Formats**: PDF, CSV, Excel, Images (OCR).
-   **Usage**: Click the "Quick Exec > Upload" area or the paperclip icon in chat. The content is extracted and added to the momentary conversation context.

## API Endpoints (`web_server.py`)

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/` | GET | Serves the main Dashboard UI. |
| `/api/chat` | POST | Unified chat endpoint. Handles intent detection, RAG search, and LLM generation. |
| `/api/stats` | GET | Proxies stats from the Pathway backend. |
| `/api/analytics` | GET | Calculates traffic and sender statistics from local files. |
| `/api/upload` | POST | Handles file uploads and text extraction. |
| `/api/scan` | POST | Simulates a security scan (Mock endpoint). |
| `/api/alerts` | GET | **[NEW]** Extracts urgent tasks using LLM analysis. |

## Configuration

-   **Context Window**: Configured in `web_server.py` (`MAX_CONTEXT_PER_SOURCE = 800`).
-   **LLM Model**: Defaults to `llama3.1` via Ollama.
-   **Port**: Web server runs on `5000`, Pathway on `8000`.
