import logging
from pathlib import Path

import pathway as pw
from dotenv import load_dotenv
from pathway.xpacks.llm.embedders import SentenceTransformerEmbedder
from pathway.xpacks.llm.splitters import RecursiveSplitter
from pathway.xpacks.llm.vector_store import VectorStoreServer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Load environment variables
load_dotenv()

# Set license key
pw.set_license_key("demo-license-key-with-telemetry")


def main():
    # Configuration
    EMAIL_PATH = "data/emails"
    PROCESSED_ATTACHMENTS_PATH = "data/emails_processed"
    PATHWAY_HOST = "127.0.0.1"
    PATHWAY_PORT = 8000
    EMBEDDING_MODEL = "mixedbread-ai/mxbai-embed-large-v1"
    
    # Check if data directories exist
    if not Path(EMAIL_PATH).exists():
        logging.warning(f"Data path {EMAIL_PATH} does not exist. Creating it...")
        Path(EMAIL_PATH).mkdir(parents=True, exist_ok=True)
    
    if not Path(PROCESSED_ATTACHMENTS_PATH).exists():
        logging.warning(f"Processed attachments path {PROCESSED_ATTACHMENTS_PATH} does not exist. Creating it...")
        Path(PROCESSED_ATTACHMENTS_PATH).mkdir(parents=True, exist_ok=True)
    
    # PROCESS ATTACHMENTS (New Logic)
    process_attachments_from_emails(EMAIL_PATH, PROCESSED_ATTACHMENTS_PATH)

    # Read email data - format must be "binary" for VectorStoreServer
    emails = pw.io.fs.read(
        path=EMAIL_PATH,
        format="binary",
        mode="streaming",
        with_metadata=True
    )
    
    # Read processed attachments (extracted text from PDFs, DOCX, CSV, etc.)
    processed_attachments = pw.io.fs.read(
        path=PROCESSED_ATTACHMENTS_PATH,
        format="binary",
        mode="streaming",
        with_metadata=True
    )
    
    # Combine both sources into one stream
    # Use promise_universes_are_disjoint() with both tables as arguments
    all_documents = emails.promise_universes_are_disjoint(
        processed_attachments
    ).concat(processed_attachments)
    
    logging.info(f"✓ Reading emails from: {EMAIL_PATH}")
    logging.info(f"✓ Reading processed attachments from: {PROCESSED_ATTACHMENTS_PATH}")
    logging.info(f"✓ Both sources will be indexed together")
    
    # Create embedder
    embedder = SentenceTransformerEmbedder(
        model=EMBEDDING_MODEL,
        call_kwargs={"show_progress_bar": False}
    )
    
    # Create text splitter
    # Recursive splitting keeps related text (paragraphs/sentences) together
    splitter = RecursiveSplitter(chunk_size=800, chunk_overlap=200)
    
    # Create vector store server
    # This will handle parsing, splitting, embedding, and serving
    vector_server = VectorStoreServer(
        all_documents,  # Now includes both emails AND processed attachments
        embedder=embedder,
        splitter=splitter,
    )
    
    logging.info(f"✓ Starting Email RAG server on {PATHWAY_HOST}:{PATHWAY_PORT}")
    logging.info(f"  Using embedding model: {EMBEDDING_MODEL}")
    logging.info("\n📡 API Endpoints:")
    logging.info(f"  - POST http://{PATHWAY_HOST}:{PATHWAY_PORT}/v1/retrieve")
    logging.info(f"       Query: {{\"query\": \"your search\", \"k\": 3}}")
    logging.info(f"  - POST http://{PATHWAY_HOST}:{PATHWAY_PORT}/v1/statistics")
    logging.info(f"  - POST http://{PATHWAY_HOST}:{PATHWAY_PORT}/v1/inputs")
    logging.info("\n📧 Indexing:")
    logging.info(f"  - Raw emails from {EMAIL_PATH}")
    logging.info(f"  - Attachment text from {PROCESSED_ATTACHMENTS_PATH}")
    logging.info(f"  - PDFs, DOCX, CSV, Excel, Images - ALL SEARCHABLE!")
    logging.info("\n⏳ Indexing documents... Please wait...")
    
    # Run the server
    # threaded=False means it will block (use True if running in notebook)
    # with_cache=True enables caching of embeddings
    vector_server.run_server(
        host=PATHWAY_HOST,
        port=PATHWAY_PORT,
        threaded=False,
        with_cache=True,
    )


def process_attachments_from_emails(email_dir: str, output_dir: str):
    """
    Scans email directory for .eml files, extracts attachments, 
    converts them to text (OCR/Pandas), and saves to output_dir.
    This runs ONCE at startup to ensure everything is indexed.
    """
    import email
    import os
    import io
    from email import policy
    
    # Optional Imports
    try: from PIL import Image; import pytesseract
    except ImportError: Image = None
    try: from pypdf import PdfReader
    except ImportError: PdfReader = None
    try: import pandas as pd
    except ImportError: pd = None

    logging.info(f"🚀 Starting Deep Attachment Processing...")
    logging.info(f"   - Input: {email_dir}")
    logging.info(f"   - Output: {output_dir}")
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    count = 0
    
    for root, _, files in os.walk(email_dir):
        for filename in files:
            if not filename.lower().endswith('.eml'):
                continue
                
            file_path = os.path.join(root, filename)
            try:
                with open(file_path, 'rb') as f:
                    msg = email.message_from_binary_file(f, policy=policy.default)
                
                # Iterate over parts
                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    if part.get('Content-Disposition') is None:
                        continue
                        
                    fname = part.get_filename()
                    if not fname: continue
                    
                    if fname:
                        # Normalize filename
                        safe_fname = "".join([c for c in fname if c.isalpha() or c.isdigit() or c in '._-']).strip()
                        out_name = f"{filename}_{safe_fname}.txt"
                        out_path = os.path.join(output_dir, out_name)
                        
                        # Skip if already processed to save time
                        if os.path.exists(out_path):
                            continue
                            
                        # Extract content
                        payload = part.get_payload(decode=True)
                        if not payload: continue
                        
                        text_content = ""
                        ext = os.path.splitext(fname)[1].lower()
                        
                        # Logic Mapping
                        if ext == '.pdf' and PdfReader:
                            try:
                                reader = PdfReader(io.BytesIO(payload))
                                text_parts = [p.extract_text() for p in reader.pages if p.extract_text()]
                                text_content = "\n".join(text_parts)
                                if not text_content: text_content = "[PDF - Scanned/Empty]"
                            except Exception as e: text_content = f"[PDF Error: {e}]"
                            
                        elif ext in ['.jpg', '.jpeg', '.png'] and Image and pytesseract:
                            try:
                                img = Image.open(io.BytesIO(payload))
                                text_content = pytesseract.image_to_string(img)
                                if not text_content.strip(): text_content = "[Image - No Text Found]"
                            except Exception as e: text_content = f"[OCR Error: {e}]"
                            
                        elif ext in ['.xlsx', '.xls', '.csv'] and pd:
                            try:
                                bio = io.BytesIO(payload)
                                if ext == '.csv': df = pd.read_csv(bio)
                                else: df = pd.read_excel(bio)
                                text_content = df.head(50).to_markdown(index=False) # Index first 50 rows
                            except Exception as e: text_content = f"[Spreadsheet Error: {e}]"
                        
                        else:
                            # Try text decode
                            try: text_content = payload.decode('utf-8')
                            except: continue # Binary/Unknown
                        
                        # WRITE TO PROCESSED FOLDER
                        if text_content:
                            final_content = f"Source Email: {filename}\nAttachment: {fname}\n\nContent:\n{text_content}"
                            with open(out_path, 'w', encoding='utf-8') as out_f:
                                out_f.write(final_content)
                            count += 1
                            logging.info(f"   [+] Filtered attachment: {fname}")

            except Exception as e:
                logging.error(f"Error processing email {filename}: {e}")
                
    logging.info(f"✅ Attachment Processing Complete. Processed {count} new attachments.")


if __name__ == "__main__":
    main()
