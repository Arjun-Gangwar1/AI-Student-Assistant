"""
RAG Chat Interface - Combines vector search with LLM responses
Supports multiple free LLM providers
NOW WITH ATTACHMENT AWARENESS!
"""

import requests
import json
import os
from typing import List, Dict
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# Configuration
PATHWAY_URL = "http://127.0.0.1:8000"
RETRIEVE_ENDPOINT = f"{PATHWAY_URL}/v1/retrieve"

# LLM Provider options (uncomment the one you want to use)
LLM_PROVIDER = "groq"  # Options: "groq", "together", "openai", "ollama"


class LLMClient:
    """Client for various LLM providers"""
    
    def __init__(self, provider: str = "groq"):
        self.provider = provider
        
        if provider == "groq":
            # Groq - Fast and free (requires API key from groq.com)
            self.api_key = os.getenv("GROQ_API_KEY")
            self.api_url = "https://api.groq.com/openai/v1/chat/completions"
            self.model = "llama-3.3-70b-versatile"  # Fast and good quality
            
        elif provider == "together":
            # Together AI - Free tier available
            self.api_key = os.getenv("TOGETHER_API_KEY")
            self.api_url = "https://api.together.xyz/v1/chat/completions"
            self.model = "meta-llama/Llama-3-70b-chat-hf"
            
        elif provider == "openai":
            # OpenAI - Paid but high quality
            self.api_key = os.getenv("OPENAI_API_KEY")
            self.api_url = "https://api.openai.com/v1/chat/completions"
            self.model = "gpt-4o-mini"  # Cheaper option
            
        elif provider == "ollama":
            # Ollama - Completely free, runs locally
            self.api_key = None
            self.api_url = "http://localhost:11434/api/chat"
            self.model = "llama3.1"  # or "mistral", "phi3", etc.
    
    def generate(self, messages: List[Dict], temperature: float = 0.7) -> str:
        """Generate response from LLM"""
        
        if self.provider == "ollama":
            # Ollama has different API format
            return self._generate_ollama(messages)
        
        # OpenAI-compatible API
        headers = {
            "Content-Type": "application/json",
        }
        
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": 1000,
        }
        
        try:
            response = requests.post(
                self.api_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]
        
        except Exception as e:
            return f"Error generating response: {e}"
    
    def _generate_ollama(self, messages: List[Dict]) -> str:
        """Generate response using Ollama local API"""
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False
        }
        
        try:
            response = requests.post(
                self.api_url,
                json=payload,
                timeout=60
            )
            response.raise_for_status()
            data = response.json()
            return data["message"]["content"]
        
        except Exception as e:
            return f"Error with Ollama: {e}. Make sure Ollama is running: 'ollama serve'"


def search_emails(query: str, k: int = 5) -> List[Dict]:
    """Search emails using Pathway vector store"""
    try:
        payload = {"query": query, "k": k}
        response = requests.post(
            RETRIEVE_ENDPOINT,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    
    except Exception as e:
        print(f"Error searching: {e}")
        return []


def identify_source_type(file_path: str) -> tuple[str, str]:
    """Identify if source is an email or attachment and extract details"""
    path = Path(file_path)
    filename = path.name
    
    # Check if it's a processed attachment
    if filename.startswith("attachment_"):
        # Format: attachment_{email_id}_{original_filename}.txt
        parts = filename.replace("attachment_", "").replace(".txt", "").split("_", 1)
        email_id = parts[0] if len(parts) > 0 else "unknown"
        original_name = parts[1] if len(parts) > 1 else "unknown"
        
        # Try to determine file type from original name
        original_ext = Path(original_name).suffix.upper()
        file_type = {
            '.PDF': 'PDF Document',
            '.DOCX': 'Word Document',
            '.DOC': 'Word Document',
            '.CSV': 'CSV Spreadsheet',
            '.XLSX': 'Excel Spreadsheet',
            '.XLS': 'Excel Spreadsheet',
            '.JPG': 'Image',
            '.JPEG': 'Image',
            '.PNG': 'Image',
            '.TXT': 'Text File'
        }.get(original_ext, 'Attachment')
        
        return "attachment", f"{file_type}: {original_name}"
    
    # Otherwise it's a regular email
    elif filename.startswith("email_"):
        return "email", "Email"
    
    return "unknown", filename


def create_rag_prompt(query: str, search_results: List[Dict]) -> str:
    """Create prompt with context from search results - NOW WITH ATTACHMENT AWARENESS"""
    
    # Separate emails and attachments
    email_contexts = []
    attachment_contexts = []
    
    for i, result in enumerate(search_results, 1):
        text = result.get("text", "")
        metadata = result.get("metadata", {})
        file_path = metadata.get('path', 'Unknown')
        
        # Identify source type
        source_type, source_description = identify_source_type(file_path)
        
        if source_type == "attachment":
            attachment_contexts.append({
                'index': i,
                'description': source_description,
                'content': text[:600],  # Longer snippet for attachments
                'path': file_path
            })
        else:
            email_contexts.append({
                'index': i,
                'description': source_description,
                'content': text[:400],
                'path': file_path
            })
    
    # Build context string
    context_parts = []
    
    if email_contexts:
        context_parts.append("=== EMAILS ===")
        for ctx in email_contexts:
            context_parts.append(
                f"\n[Source {ctx['index']}] {ctx['description']}\n"
                f"Content: {ctx['content']}...\n"
            )
    
    if attachment_contexts:
        context_parts.append("\n=== ATTACHMENTS ===")
        for ctx in attachment_contexts:
            context_parts.append(
                f"\n[Source {ctx['index']}] {ctx['description']}\n"
                f"Content: {ctx['content']}...\n"
            )
    
    context = "\n".join(context_parts)
    
    prompt = f"""You are a helpful email assistant with access to both emails and their attachments (PDFs, documents, spreadsheets, images).

Context from Search Results:
{context}

User Question: {query}

Instructions:
- Answer based ONLY on the provided context (emails and attachments)
- Be concise and specific
- If the context doesn't contain relevant information, say so
- Include relevant details like dates, numbers, names, or data from the sources
- When referencing information, mention whether it came from an email or an attachment (e.g., "According to the PDF attachment..." or "In the email from...")
- If data comes from a spreadsheet (CSV/Excel), mention that specifically
- Format your response in a clear, readable way

Answer:"""
    
    return prompt


def chat(query: str, llm_client: LLMClient, k: int = 5, verbose: bool = True):
    """Main RAG chat function"""
    
    if verbose:
        print(f"\n🔍 Searching emails and attachments for: '{query}'")
    
    # Step 1: Search vector store
    search_results = search_emails(query, k=k)
    
    if not search_results:
        return "No relevant emails or attachments found for your query."
    
    if verbose:
        # Analyze results
        emails_found = sum(1 for r in search_results if "email_" in r.get("metadata", {}).get("path", ""))
        attachments_found = sum(1 for r in search_results if "attachment_" in r.get("metadata", {}).get("path", ""))
        
        print(f"✓ Found {len(search_results)} relevant sources:")
        if emails_found:
            print(f"  - {emails_found} email(s)")
        if attachments_found:
            print(f"  - {attachments_found} attachment(s) (PDFs, documents, spreadsheets)")
    
    # Step 2: Create RAG prompt
    prompt = create_rag_prompt(query, search_results)
    
    if verbose:
        print(f"🤖 Generating response using {llm_client.provider}...")
    
    # Step 3: Generate response
    messages = [
        {
            "role": "system",
            "content": "You are a helpful email assistant that answers questions based on email content and attachments (PDFs, documents, spreadsheets, images)."
        },
        {
            "role": "user",
            "content": prompt
        }
    ]
    
    response = llm_client.generate(messages)
    
    # Step 4: Add source information if verbose
    if verbose:
        print("\n" + "─" * 70)
        print("📎 Sources used:")
        print("─" * 70)
        for result in search_results:
            path = result.get("metadata", {}).get("path", "unknown")
            source_type, source_desc = identify_source_type(path)
            icon = "📧" if source_type == "email" else "📎"
            print(f"{icon} {source_desc}")
    
    return response


def interactive_chat(llm_client: LLMClient):
    """Interactive chat loop"""
    print("=" * 70)
    print("          Email RAG Chat - Interactive Mode")
    print("          NOW WITH ATTACHMENT SUPPORT!")
    print("=" * 70)
    print(f"Using LLM: {llm_client.provider} ({llm_client.model})")
    print("\n📚 Can search through:")
    print("  - Emails")
    print("  - PDF documents")
    print("  - Word documents (DOCX)")
    print("  - Spreadsheets (CSV, Excel)")
    print("  - Images (with OCR)")
    print("\nType 'quit' or 'exit' to stop")
    print("=" * 70)
    
    while True:
        print("\n" + "─" * 70)
        user_query = input("\n💬 Your question: ").strip()
        
        if user_query.lower() in ['quit', 'exit', 'q']:
            print("\n👋 Goodbye!")
            break
        
        if not user_query:
            continue
        
        response = chat(user_query, llm_client, k=5, verbose=True)
        
        print("\n" + "─" * 70)
        print("📧 Response:")
        print("─" * 70)
        print(response)


def main():
    """Main function with example queries"""
    
    # Initialize LLM client
    print(f"Initializing {LLM_PROVIDER} client...")
    llm_client = LLMClient(provider=LLM_PROVIDER)
    
    # Check if API key is needed
    if LLM_PROVIDER in ["groq", "together", "openai"] and not llm_client.api_key:
        print(f"\n⚠️  Warning: {LLM_PROVIDER.upper()}_API_KEY not found in environment variables")
        print(f"\nTo use {LLM_PROVIDER}:")
        print(f"1. Get API key from their website")
        print(f"2. Add to .env file: {LLM_PROVIDER.upper()}_API_KEY=your_key_here")
        print(f"\nOr switch to Ollama (free, runs locally):")
        print(f"1. Install: https://ollama.ai")
        print(f"2. Run: ollama pull llama3.1")
        print(f"3. Start: ollama serve")
        print(f"4. Change LLM_PROVIDER to 'ollama' in this script")
        return
    
    # Run a few example queries first
    print("\n" + "=" * 70)
    print("Running example queries...")
    print("=" * 70)
    
    example_queries = [
        "What PDFs were attached to recent emails?",
        "Show me data from any spreadsheets",
        "What does the financial report say about revenue?",
    ]
    
    for query in example_queries:
        print(f"\n\n{'='*70}")
        print(f"Question: {query}")
        print('='*70)
        response = chat(query, llm_client, k=3, verbose=False)
        print(response)
    
    # Start interactive mode
    print("\n\n")
    interactive_chat(llm_client)


if __name__ == "__main__":
    main()