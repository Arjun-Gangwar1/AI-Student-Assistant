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

app = Flask(__name__, static_folder='static')
CORS(app)

# Configuration
PATHWAY_URL = "http://127.0.0.1:8000"
LLM_API_URL = "http://localhost:11434/api/chat"
LLM_MODEL = "llama3.1"

# CRITICAL: Reduced context to prevent crashes
MAX_CONTEXT_PER_SOURCE = 800  # Reduced from 3000
MAX_SOURCES = 3  # Reduced from 5


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


# 1. Increase search depth to 15 to ensure we find filtered matches
PATHWAY_URL = "http://127.0.0.1:8000"
SEARCH_K = 15 

def apply_metadata_filters(results: list, sender_filter: str = None, has_attachments: bool = None) -> list:
    filtered = []
    for result in results:
        text = result.get('text', '')
        metadata = result.get('metadata', {})
        path = metadata.get('path', '')

        # SENDER FILTER: Use case-insensitive regex for "From: <name>"
        if sender_filter:
            if not re.search(f"From:.*{re.escape(sender_filter)}", text, re.IGNORECASE):
                continue

        # ATTACHMENT FILTER: Check filename prefix
        is_attachment = 'attachment_' in path
        if has_attachments is True and not is_attachment:
            continue
        if has_attachments is False and is_attachment:
            continue

        filtered.append(result)
    return filtered[:5] # Return top 5 AFTER filtering


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
        
        print(f"\n[API] Received query: {query}")
        
        # Extract filters
        sender_filter = data.get('sender_filter')
        date_filter = data.get('date_filter')
        has_attachments = data.get('has_attachments')
        
        # Detect if we need to search emails
        needs_search = detect_search_intent(query)
        print(f"[API] Search needed: {needs_search}")
        
        if not needs_search:
            # Just conversation - no search
            messages = [
                {
                    "role": "system",
                    "content": "You are a friendly email assistant. Respond warmly and concisely to greetings and casual chat. Let users know you can help search their emails."
                },
                {
                    "role": "user",
                    "content": query
                }
            ]
            
            response_text, error = safe_llm_call(messages, timeout=30)
            
            if error:
                return jsonify({
                    "response": f"Error: {error}",
                    "sources": [],
                    "searched": False
                }), 500
            
            return jsonify({
                "response": response_text,
                "sources": [],
                "searched": False
            })
        
        # Search is needed
        enhanced_query = query
        if sender_filter:
            enhanced_query = f"{query} from:{sender_filter}"
        if has_attachments:
            enhanced_query = f"{query} has attachments"
        
        print(f"[API] Searching Pathway with: {enhanced_query}")
        
        # Search emails
        try:
            search_response = requests.post(
                f"{PATHWAY_URL}/v1/retrieve",
                json={"query": enhanced_query, "k": 5},
                timeout=10
            )
            search_response.raise_for_status()
            search_results = search_response.json()
            print(f"[API] Pathway returned {len(search_results)} results")
        except Exception as e:
            print(f"[API] Pathway search failed: {e}")
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
        
        if not filtered_results:
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
        
        # Build COMPACT context
        context_parts = []
        
        for i, result in enumerate(filtered_results, 1):
            text = result.get('text', '')
            metadata = result.get('metadata', {})
            path = metadata.get('path', 'Unknown')
            
            # Identify source type
            if 'attachment_' in path:
                source_type = "Attachment"
            else:
                source_type = "Email"
            
            # CRITICAL: Use reduced context length
            snippet = text[:MAX_CONTEXT_PER_SOURCE]
            context_parts.append(f"[Source {i}] {source_type}\n{snippet}...")
        
        context = "\n\n".join(context_parts)
        
        print(f"[API] Context length: {len(context)} chars")
        
        # Create CONCISE RAG prompt
        prompt = f"""Context from emails/attachments:
{context}

Question: {query}

Instructions:
- Answer based ONLY on the context above
- Be concise and specific
- Cite sources (e.g., "In Source 1...")
- If context lacks info, say "I couldn't find that in your emails"

Answer:"""
        
        # Generate response
        messages = [
            {
                "role": "system",
                "content": "You are a helpful email assistant. Answer questions based on provided email content."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        print(f"[API] Calling LLM with {len(prompt)} char prompt...")
        response_text, error = safe_llm_call(messages, timeout=90)
        
        if error:
            return jsonify({
                "response": f"Sorry, I encountered an error: {error}",
                "sources": [],
                "searched": True
            }), 500
        
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
            "searched": True
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
