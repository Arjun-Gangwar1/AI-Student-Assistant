"""
Flask Web Server for Email RAG Dashboard - FIXED VERSION
Optimized context length and better error handling
"""

from flask import Flask, send_from_directory, jsonify, request
from flask_cors import CORS
import requests
import os
from pathlib import Path
import re
import io
from datetime import datetime
try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None
try:
    from PIL import Image
    import pytesseract
except ImportError:
    Image = None
    pytesseract = None

app = Flask(__name__, static_folder='static')
CORS(app)

# Configuration
PATHWAY_URL = "http://127.0.0.1:8000"
LLM_API_URL = "http://localhost:11434/api/chat"
LLM_MODEL = "llama3.1"

# CRITICAL: Reduced context to prevent crashes
MAX_CONTEXT_PER_SOURCE = 800  # Reduced from 3000
MAX_SOURCES = 15  # Reduced from 5


def detect_search_intent(query: str) -> bool:
    """
    Detect if the user wants to search emails or just chat
    Returns True if search is needed, False for general conversation
    """
    query_lower = query.lower().strip()
    
    # Greetings and casual conversation - NO SEARCH
    casual_patterns = [
        r'^(hi|hey|hello|howdy|greetings)[\s\?!]*$',
        r'^(good morning|good afternoon|good evening)[\s\?!]*$',
        r'^(how are you|how\'s it going|what\'s up|sup)[\s\?!]*$',
        r'^(thanks|thank you|thx)[\s\?!]*$',
        r'^(bye|goodbye|see you|cya)[\s\?!]*$',
        r'^(ok|okay|alright|got it|understood)[\s\?!]*$',
    ]
    
    for pattern in casual_patterns:
        if re.match(pattern, query_lower):
            return False
    
    # Questions about the assistant itself - NO SEARCH
    if any(word in query_lower for word in ['what can you do', 'who are you', 'what are you', 'help me', 'how do you work']):
        if 'email' not in query_lower and 'mail' not in query_lower:
            return False
    
    # Email-related keywords - SEARCH NEEDED
    email_keywords = [
        'email', 'mail', 'message', 'inbox', 'sent', 'received',
        'attachment', 'pdf', 'document', 'file', 'spreadsheet', 'excel', 'csv',
        'from', 'to', 'subject', 'dated', 'yesterday', 'last week', 'recent',
        'find', 'search', 'show', 'get', 'what', 'who', 'when', 'where',
        'tell me about', 'information about', 'details on', 'summary of'
    ]
    
    has_email_keywords = any(keyword in query_lower for keyword in email_keywords)
    question_words = ['what', 'who', 'when', 'where', 'which', 'how many', 'tell me', 'show me', 'find', 'get', 'list']
    has_question = any(word in query_lower for word in question_words)
    
    return has_email_keywords or has_question


def apply_metadata_filters(results: list, sender_filter: str = None, 
                          date_filter: str = None, has_attachments: bool = None) -> list:
    """Post-process search results with metadata filters"""
    filtered = []
    
    for result in results:
        text = result.get('text', '')
        
        # Sender filter
        if sender_filter:
            from_line = None
            for line in text.split('\n'):
                if line.startswith('From: '):
                    from_line = line
                    break
            
            if from_line:
                if sender_filter.lower() not in from_line.lower():
                    continue
            else:
                continue
        
        # Attachment filter
        if has_attachments is not None:
            att_line = None
            for line in text.split('\n'):
                if line.startswith('Attachments: '):
                    att_line = line
                    break
            
            if att_line:
                try:
                    att_count = int(att_line.split(': ')[1])
                    if has_attachments and att_count == 0:
                        continue
                    if not has_attachments and att_count > 0:
                        continue
                except:
                    pass
        
        filtered.append(result)
    
    return filtered


def safe_llm_call(messages, timeout=90):
    """Make LLM call with proper error handling"""
    try:
        print(f"[LLM] Making request to {LLM_API_URL}...")
        response = requests.post(
            LLM_API_URL,
            json={
                "model": LLM_MODEL,
                "messages": messages,
                "stream": False,
                "options": {
                    "temperature": 0.7,
                    "num_ctx": 4096  # Limit context window
                }
            },
            timeout=timeout
        )
        
        print(f"[LLM] Response status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"[LLM] Error response: {response.text}")
            return None, f"LLM returned status {response.status_code}"
        
        result = response.json()
        content = result.get('message', {}).get('content', '')
        
        if not content:
            print(f"[LLM] Empty response: {result}")
            return None, "LLM returned empty response"
        
        print(f"[LLM] Success! Response length: {len(content)} chars")
        return content, None
        
    except requests.exceptions.Timeout:
        print(f"[LLM] Timeout after {timeout} seconds")
        return None, f"LLM request timed out after {timeout} seconds. Try a simpler question."
    except requests.exceptions.ConnectionError:
        print("[LLM] Connection error - is Ollama running?")
        return None, "Cannot connect to Ollama. Make sure it's running: 'ollama serve'"
    except Exception as e:
        print(f"[LLM] Unexpected error: {e}")
        return None, f"LLM error: {str(e)}"


@app.route('/')
def index():
    """Serve the main dashboard"""
    return send_from_directory('static', 'index.html')


@app.route('/api/chat', methods=['POST'])
def chat():
    """Unified chat endpoint with intent detection"""
    try:
        data = request.json
        query = data.get('query', '').strip()
        local_context = data.get('local_context', '')
        history = data.get('history', [])
        
        print(f"\n[API] Received query: {query}")
        if history:
            print(f"[API] History length: {len(history)}")
        
        # Extract filters
        sender_filter = data.get('sender_filter')
        date_filter = data.get('date_filter')
        has_attachments = data.get('has_attachments')
        
        # Detect if we need to search emails
        needs_search = detect_search_intent(query)
        print(f"[API] Search needed: {needs_search}")

        # System Prompt
        current_time = datetime.now().strftime("%A, %B %d, %Y")
        time_context = f"TODAY'S DATE: {current_time}. Use this to resolve relative dates like 'yesterday' or 'last week'."

        system_base = (
            f"{time_context}\n"
            "You are a friendly email assistant. Respond warmly and concisely. "
            "If the user explicitly states their name (e.g., 'My name is John', 'Call me Sarah'), "
            "start your response with the tag '[NAME_UPDATE: Name]' followed by your normal reply. "
            "Example: '[NAME_UPDATE: John] Nice to meet you, John!'"
        )

        valid_history = [{"role": m["role"], "content": m["content"]} for m in history if m.get("content")][-10:]
        
        # If we have local context, we should proceed to generation even if we don't need to search emails
        if not needs_search and not local_context:
            # Just conversation - no search AND no local context
            messages = [{"role": "system", "content": system_base}] + valid_history + [{"role": "user", "content": query}]
            
            response_text, error = safe_llm_call(messages, timeout=30)
            
            if error:
                return jsonify({
                    "response": f"Error: {error}",
                    "sources": [],
                    "searched": False
                }), 500
            
            # Check for name update
            new_name = None
            if response_text and "[NAME_UPDATE:" in response_text:
                try:
                    match = re.search(r'\[NAME_UPDATE:\s*(.*?)\]', response_text)
                    if match:
                        new_name = match.group(1).strip()
                        response_text = response_text.replace(match.group(0), "").strip()
                except: pass

            return jsonify({
                "response": response_text,
                "sources": [],
                "searched": False,
                "new_username": new_name
            })
        
        # Search is needed OR we have local context
        enhanced_query = query
        if sender_filter:
            enhanced_query = f"{query} from:{sender_filter}"
        if has_attachments:
            enhanced_query = f"{query} has attachments"
        
        search_results = []
        if needs_search:
            print(f"[API] Searching Pathway with: {enhanced_query}")
            
            # Search emails
            try:
                search_response = requests.post(
                    f"{PATHWAY_URL}/v1/retrieve",
                    json={"query": enhanced_query, "k": 15},
                    timeout=10
                )
                search_response.raise_for_status()
                search_results = search_response.json()
                print(f"[API] Pathway returned {len(search_results)} results")
            except Exception as e:
                print(f"[API] Pathway search failed: {e}")
                if not local_context: # Only fail if we don't have local context
                    return jsonify({
                        "response": f"Search error: {str(e)}. Make sure app.py is running.",
                        "sources": [],
                        "searched": True
                    }), 500
        
        # Apply metadata filters
        filtered_results = apply_metadata_filters(
            search_results,
            sender_filter=sender_filter,
            date_filter=date_filter,
            has_attachments=has_attachments
        )
        
        # Take only top MAX_SOURCES
        filtered_results = filtered_results[:MAX_SOURCES]
        print(f"[API] Using {len(filtered_results)} filtered results")
        
        if not filtered_results and not local_context:
            messages = [
                {
                    "role": "system",
                    "content": "You are a helpful email assistant."
                },
                {
                    "role": "user",
                    "content": f"I searched for '{query}' but found no relevant emails. Politely tell the user and suggest they try rephrasing."
                }
            ]
            
            response_text, error = safe_llm_call(messages, timeout=30)
            
            if error:
                response_text = "Sorry, I couldn't find any relevant emails for your query."
            
            return jsonify({
                "response": response_text,
                "sources": [],
                "searched": True
            })
        
        # Build Context
        context_parts = []
        for i, result in enumerate(filtered_results, 1):
            text = result.get('text', '')[:MAX_CONTEXT_PER_SOURCE]
            path = result.get('metadata', {}).get('path', 'Unknown')
            source_type = "Attachment" if 'attachment_' in path else "Email"
            context_parts.append(f"[Source {i}] {source_type}\n{text}...")
        
        context = "\n\n".join(context_parts)
        if local_context: context = f"Uploaded File Content:\n{local_context}\n\n" + context

        # Update System Prompt with Context (Backsourced)
        # This keeps the history clean and strictly conversational
        system_with_context = system_base
        if context:
            system_with_context += f"\n\n### RELEVANT DATA / CONTEXT ###\n{context}\n\n### INSTRUCTIONS ###\nAnswer the user's question based on the above context if relevant. If not found, say so."

        # Generate response
        # Messages = [ System(Context) ] + History + [ User(Query) ]
        messages = [{"role": "system", "content": system_with_context}] + valid_history + [{"role": "user", "content": query}]
        
        print(f"[API] Calling LLM with {len(system_with_context)} char system context...")
        response_text, error = safe_llm_call(messages, timeout=90)
        
        if error:
            return jsonify({
                "response": f"Sorry, I encountered an error: {error}",
                "sources": [],
                "searched": True
            }), 500
        
        # Check for name update
        new_name = None
        if response_text and "[NAME_UPDATE:" in response_text:
            try:
                match = re.search(r'\[NAME_UPDATE:\s*(.*?)\]', response_text)
                if match:
                    new_name = match.group(1).strip()
                    response_text = response_text.replace(match.group(0), "").strip()
            except: pass

        return jsonify({
            "response": response_text,
            "sources": [
                {
                    "index": i + 1,
                    "path": r.get('metadata', {}).get('path', 'Unknown'),
                    "type": 'attachment' if 'attachment_' in r.get('metadata', {}).get('path', '') else 'email',
                    "description": f"Source {i + 1}"
                }
                for i, r in enumerate(filtered_results)
            ],
            "searched": True,
            "new_username": new_name
        })
    
    except Exception as e:
        print(f"[API] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        
        return jsonify({
            "error": str(e),
            "response": f"Sorry, I encountered an error: {str(e)}",
            "sources": [],
            "searched": False
        }), 500


@app.route('/api/stats', methods=['GET'])
def stats():
    """Get statistics from Pathway"""
    try:
        response = requests.post(
            f"{PATHWAY_URL}/v1/statistics",
            json={},
            timeout=5
        )
        response.raise_for_status()
        return jsonify(response.json())
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/analytics', methods=['GET'])
def analytics():
    """Get detailed email analytics (Traffic & Top Senders)"""
    try:
        email_dir = Path("data/emails")
        if not email_dir.exists():
            return jsonify({"traffic": [], "senders": []})

        from collections import Counter
        from datetime import datetime, timedelta
        
        # Robust date parsing
        try:
            import dateutil.parser as dparser
            parse_date = lambda d: dparser.parse(d)
        except ImportError:
            # Fallback: simple string matching if lib is missing
            parse_date = lambda d: datetime.strptime(d.strip(), "%a, %d %b %Y %H:%M:%S %z")

        dates = []
        senders = []
        
        # Scan last 500 emails to keep it fast
        files = sorted(list(email_dir.glob("*.txt")), key=os.path.getmtime, reverse=True)[:500]
        
        for f in files:
            try:
                content = f.read_text(encoding='utf-8', errors='ignore')
                
                # Extract Date
                date_line = next((l for l in content.split('\n') if l.startswith('Date: ')), None)
                if date_line:
                    raw_date = date_line.replace('Date: ', '').strip()
                    try:
                        # Try parsing
                        dt = parse_date(raw_date)
                        dates.append(dt.strftime("%Y-%m-%d"))
                    except: pass
                
                # Extract Sender
                from_line = next((l for l in content.split('\n') if l.startswith('From: ')), None)
                if from_line:
                    sender = from_line.replace('From: ', '').strip()
                    # Simplify sender name (remove email <...>)
                    if '<' in sender: sender = sender.split('<')[0].strip().replace('"', '')
                    senders.append(sender)
                    
            except: continue

        # 1. Traffic Volume (Last 7 Days)
        today = datetime.now()
        last_7_days = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(6, -1, -1)]
        date_counts = Counter(dates)
        traffic_data = [{"date": d, "count": date_counts.get(d, 0)} for d in last_7_days]

        # 2. Top Senders
        sender_counts = Counter(senders).most_common(5)
        sender_data = [{"name": s[0], "count": s[1]} for s in sender_counts]

        return jsonify({
            "traffic": traffic_data,
            "senders": sender_data
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


try:
    import pandas as pd
except ImportError:
    pd = None

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Handle file upload and extract text"""
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file part"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400
            
        filename = file.filename
        file_ext = os.path.splitext(filename)[1].lower()
        extracted_text = ""
        
        print(f"[API] Processing upload: {filename}")
        
        # --- PDF Handling ---
        if file_ext == '.pdf':
            if not PdfReader:
                return jsonify({"text": f"Error: pypdf not installed, cannot read PDF: {filename}", "filename": filename})
            try:
                pdf_stream = io.BytesIO(file.read())
                reader = PdfReader(pdf_stream)
                text_parts = []
                for i, page in enumerate(reader.pages):
                    text = page.extract_text()
                    if text:
                        text_parts.append(f"[Page {i+1}] {text}")
                extracted_text = "\n".join(text_parts)
                if not extracted_text:
                    extracted_text = f"[PDF {filename} processed but no text found - maybe scanned?]"
            except Exception as e:
                print(f"PDF extraction error: {e}")
                extracted_text = f"Error reading PDF {filename}: {str(e)}"
                
        # --- Image Handling ---
        elif file_ext in ['.jpg', '.jpeg', '.png']:
            if not Image or not pytesseract:
                 extracted_text = f"[Image {filename} uploaded. OCR not available on server.]"
            else:
                try:
                    image_stream = io.BytesIO(file.read())
                    image = Image.open(image_stream)
                    extracted_text = pytesseract.image_to_string(image)
                    if not extracted_text.strip():
                        extracted_text = f"[Image {filename} processed but no text found]"
                except Exception as e:
                     print(f"OCR error: {e}")
                     extracted_text = f"Error reading image {filename}: {str(e)}"
                     
        # --- Excel/CSV Handling ---
        elif file_ext in ['.xlsx', '.xls', '.csv']:
            if not pd:
                extracted_text = f"[{filename} uploaded. Pandas not installed on server, cannot read spreadsheet.]"
            else:
                try:
                    file_stream = io.BytesIO(file.read())
                    if file_ext == '.csv':
                        df = pd.read_csv(file_stream)
                    else:
                        df = pd.read_excel(file_stream)
                    
                    # Convert to string representation
                    extracted_text = f"[Spreadsheet Analysis: {filename}]\n\n"
                    extracted_text += f"Columns: {', '.join(df.columns)}\n"
                    extracted_text += f"Rows: {len(df)}\n\nSample Data:\n"
                    extracted_text += df.head(10).to_markdown(index=False)
                except Exception as e:
                    print(f"Spreadsheet error: {e}")
                    extracted_text = f"Error reading spreadsheet {filename}: {str(e)}"

        # --- Text/Other Handling ---
        else:
             try:
                 extracted_text = file.read().decode('utf-8')
             except:
                 extracted_text = f"[File {filename} uploaded but format not supported for text extraction]"

        print(f"[API] Extracted {len(extracted_text)} chars from {filename}")
        return jsonify({"text": extracted_text, "filename": filename})

    except Exception as e:
        print(f"Upload error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/scan', methods=['POST'])
def scan():
    """Simulate a deep security scan"""
    try:
        # Simulate some processing time and return a report
        import time
        import random
        
        # Realistically, we would trigger an LLM scan of recent emails here
        # For the hackathon/demo, we return a structured report
        
        threats = [
             "Phishing attempt detected in 'Urgent Invoice' from unknown sender.",
             "Suspicious link found in 'Package Delivery' notification.",
             "Unencrypted sensitive data found in 'Password Reset' reply."
        ]
        
        safe_messages = [
            "Scan complete. No vital threats found.",
            "System integrity: 98%. Minor anomalies in spam folder.",
            "All outbound traffic is encrypted and secure."
        ]
        
        # Random outcome
        is_threat = random.random() > 0.7
        
        steps = [
            "Initializing heuristic engine...",
            "Scanning header metadata...",
            "Analyzing attachment signatures...",
            "Cross-referencing blacklists...",
            "Finalizing report..."
        ]
        
        report = random.choice(threats) if is_threat else random.choice(safe_messages)
        status = "THREAT DETECTED" if is_threat else "SECURE"
        
        return jsonify({
            "steps": steps,
            "result": report,
            "status": status,
            "timestamp": "Now"
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    """Extract urgent deadlines/tasks from recent emails"""
    try:
        # 1. Search for urgent context
        search_query = "urgent deadline due date ASAP important"
        print(f"[API] Searching for alerts with: {search_query}")
        
        try:
            search_response = requests.post(
                f"{PATHWAY_URL}/v1/retrieve",
                json={"query": search_query, "k": 10},
                timeout=10
            )
            search_response.raise_for_status()
            results = search_response.json()
        except Exception as e:
            print(f"[API] Alert search failed: {e}")
            return jsonify([]) # Return empty on search failure

        if not results:
            return jsonify([])

        # 2. Prepare Context for LLM
        context_parts = []
        for i, result in enumerate(results, 1):
            text = result.get('text', '')[:1000]
            context_parts.append(f"[Email {i}] {text}...")
        
        context = "\n\n".join(context_parts)
        
        # 3. LLM Extraction
        prompt = (
            "Analyze the following emails and extract a list of URGENT tasks or deadlines. "
            "Return ONLY a JSON array of objects with keys: 'task' (short description), 'status' (CRITICAL/URGENT/PENDING), and 'time' (due date or estimated time). "
            "Limit to top 3-5 most important items. "
            "If no urgent items are found, return an empty array []. "
            "Do NOT return markdown formatting, just the raw JSON string.\n\n"
            f"EMAILS:\n{context}"
        )
        
        messages = [{"role": "user", "content": prompt}]
        
        response_text, error = safe_llm_call(messages, timeout=45)
        
        if error or not response_text:
            print(f"[API] Alert extraction failed: {error}")
            return jsonify([])

        # 4. Parse JSON
        import json
        try:
            # Clean up potential markdown code blocks
            clean_text = response_text.replace('```json', '').replace('```', '').strip()
            alerts = json.loads(clean_text)
            return jsonify(alerts)
        except json.JSONDecodeError:
            print(f"[API] Failed to parse LLM alert JSON: {response_text}")
            return jsonify([])
            
    except Exception as e:
        print(f"[API] Alerts error: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    static_dir = Path('static')
    static_dir.mkdir(exist_ok=True)
    
    print("=" * 70)
    print("          🌐 Email RAG Dashboard Server (FIXED)")
    print("=" * 70)
    print(f"\n✅ Server starting on http://localhost:5000")
    print(f"✅ Pathway backend: {PATHWAY_URL}")
    print(f"✅ LLM backend: {LLM_API_URL} ({LLM_MODEL})")
    print(f"\n🔧 Optimizations:")
    print(f"   ✓ Max context per source: {MAX_CONTEXT_PER_SOURCE} chars")
    print(f"   ✓ Max sources: {MAX_SOURCES}")
    print(f"   ✓ Better error handling and logging")
    print(f"   ✓ Extended timeouts for LLM calls")
    print(f"\n⚠️  Make sure Ollama is running: 'ollama serve'")
    print(f"⚠️  Make sure app.py is running for email search")
    print(f"\n📝 Check console for detailed logs")
    print("=" * 70 + "\n")
    
    app.run(host='0.0.0.0', port=5000, debug=True)
