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
    splitter = RecursiveSplitter(chunk_size=1200, chunk_overlap=150)
    
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


if __name__ == "__main__":
    main()
