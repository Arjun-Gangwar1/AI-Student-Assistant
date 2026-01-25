"""
Gmail Watcher - Continuously monitors Gmail for new emails
Automatically downloads new emails to data/emails/ folder
Now with smart attachment handling - extracts text immediately!
Pathway will automatically index them in real-time
"""

import os
import base64
import pickle
import time
from pathlib import Path
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Gmail API scopes
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# Configuration
OUTPUT_DIR = Path("data/emails")
ATTACHMENTS_DIR = Path("data/attachments")
PROCESSED_DIR = Path("data/emails_processed")  # Text extracted from attachments
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Track which emails we've already downloaded
TRACKING_FILE = Path("gmail_sync_state.txt")

# ===== ATTACHMENT CONFIGURATION =====
ATTACHMENT_CONFIG = {
    'enabled': True,
    'max_file_size_mb': 10,
    'allowed_extensions': ['.pdf', '.docx', '.txt', '.csv', '.xlsx', '.xls', '.jpg', '.jpeg', '.png'],
    'pdf_max_pages': 20,  # Skip PDFs with more pages (requires PyPDF2)
    'save_metadata_only_if_too_large': True,  # Save .meta file for skipped attachments
    'extract_text_immediately': True,  # NEW: Extract text during download (no separate processor needed!)
}


# ===== TEXT EXTRACTION FUNCTIONS =====

def extract_pdf_text(file_path: Path) -> str:
    """Extract text from PDF"""
    try:
        import PyPDF2
        text_parts = []
        with open(file_path, 'rb') as f:
            pdf_reader = PyPDF2.PdfReader(f)
            for page_num, page in enumerate(pdf_reader.pages, 1):
                page_text = page.extract_text()
                if page_text.strip():
                    text_parts.append(f"--- Page {page_num} ---\n{page_text}")
        return "\n\n".join(text_parts) if text_parts else None
    except ImportError:
        logger.warning("PyPDF2 not installed - can't extract PDF text")
        return None
    except Exception as e:
        logger.error(f"Error extracting PDF: {e}")
        return None


def extract_docx_text(file_path: Path) -> str:
    """Extract text from DOCX"""
    try:
        import docx
        doc = docx.Document(file_path)
        text_parts = [para.text for para in doc.paragraphs if para.text.strip()]
        
        # Extract tables
        for table in doc.tables:
            table_text = "TABLE:\n"
            for row in table.rows:
                row_text = " | ".join(cell.text.strip() for cell in row.cells)
                table_text += row_text + "\n"
            text_parts.append(table_text)
        
        return "\n\n".join(text_parts) if text_parts else None
    except ImportError:
        logger.warning("python-docx not installed - can't extract DOCX text")
        return None
    except Exception as e:
        logger.error(f"Error extracting DOCX: {e}")
        return None


def extract_csv_text(file_path: Path) -> str:
    """Extract text from CSV"""
    try:
        import pandas as pd
        df = pd.read_csv(file_path)
        text_parts = [
            f"CSV File: {file_path.name}",
            f"Rows: {len(df)}, Columns: {len(df.columns)}",
            f"\nColumns: {', '.join(df.columns.tolist())}",
            f"\nFirst 10 rows:\n",
            df.head(10).to_string(),
        ]
        
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            text_parts.append("\n\nNumeric Summary:")
            text_parts.append(df[numeric_cols].describe().to_string())
        
        return "\n".join(text_parts)
    except ImportError:
        logger.warning("pandas not installed - can't extract CSV text")
        return None
    except Exception as e:
        logger.error(f"Error extracting CSV: {e}")
        return None


def extract_excel_text(file_path: Path) -> str:
    """Extract text from Excel"""
    try:
        import pandas as pd
        excel_file = pd.ExcelFile(file_path)
        text_parts = [f"Excel File: {file_path.name}"]
        
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            text_parts.append(f"\n{'='*60}")
            text_parts.append(f"Sheet: {sheet_name}")
            text_parts.append(f"Rows: {len(df)}, Columns: {len(df.columns)}")
            text_parts.append(f"Columns: {', '.join(df.columns.tolist())}")
            text_parts.append(f"\nFirst 10 rows:\n{df.head(10).to_string()}")
        
        return "\n".join(text_parts)
    except ImportError:
        logger.warning("pandas/openpyxl not installed - can't extract Excel text")
        return None
    except Exception as e:
        logger.error(f"Error extracting Excel: {e}")
        return None


def extract_image_text(file_path: Path) -> str:
    """Extract text from image using OCR"""
    try:
        from PIL import Image
        import pytesseract
        image = Image.open(file_path)
        text = pytesseract.image_to_string(image)
        return f"Image OCR Text:\n{text}" if text.strip() else "Image contains no readable text"
    except ImportError:
        logger.warning("pillow/pytesseract not installed - can't extract image text")
        return None
    except Exception as e:
        logger.error(f"Error extracting image text: {e}")
        return None


def extract_text_file(file_path: Path) -> str:
    """Extract text from plain text file"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading text file: {e}")
        return None


def authenticate_gmail():
    """Authenticate with Gmail API"""
    creds = None
    
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists('credentials.json'):
                print("\n❌ credentials.json not found!")
                print("Run: python fetch_gmail.py first to set up authentication")
                return None
            
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return creds


def load_synced_ids():
    """Load set of already-synced email IDs"""
    if TRACKING_FILE.exists():
        with open(TRACKING_FILE, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_synced_id(email_id):
    """Save a synced email ID"""
    with open(TRACKING_FILE, 'a') as f:
        f.write(f"{email_id}\n")


def get_email_body(payload):
    """Extract email body from message payload"""
    body = ""
    
    if 'parts' in payload:
        for part in payload['parts']:
            if part['mimeType'] == 'text/plain':
                if 'data' in part['body']:
                    body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                    break
            elif 'parts' in part:
                body = get_email_body(part)
                if body:
                    break
    elif 'body' in payload and 'data' in payload['body']:
        body = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
    
    return body


def get_pdf_page_count(file_path):
    """Get number of pages in a PDF (requires PyPDF2)"""
    try:
        import PyPDF2
        with open(file_path, 'rb') as f:
            pdf_reader = PyPDF2.PdfReader(f)
            return len(pdf_reader.pages)
    except ImportError:
        print("      ⚠️  PyPDF2 not installed - can't check PDF page count")
        print("      Install with: pip install PyPDF2")
        return None
    except Exception as e:
        print(f"      ⚠️  Error reading PDF: {e}")
        return None


def save_attachment_metadata(attachment_info, email_id):
    """Save metadata file for skipped attachments"""
    metadata_filename = f"{attachment_info['filename']}.meta"
    metadata_path = ATTACHMENTS_DIR / f"email_{email_id}" / metadata_filename
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    
    metadata_content = f"""Attachment Metadata (File Too Large)
Email ID: {email_id}
Filename: {attachment_info['filename']}
Size: {attachment_info['size_mb']:.2f} MB
MIME Type: {attachment_info['mime_type']}
Attachment ID: {attachment_info['attachment_id']}
Reason Skipped: {attachment_info['skip_reason']}
"""
    
    with open(metadata_path, 'w', encoding='utf-8') as f:
        f.write(metadata_content)
    
    print(f"      📋 Saved metadata: {metadata_filename}")


def process_attachments(service, message_id, payload, email_subject):
    """Process and download attachments from an email - NOW WITH IMMEDIATE TEXT EXTRACTION!"""
    
    if not ATTACHMENT_CONFIG['enabled']:
        return []
    
    attachments_info = []
    
    def extract_attachments(parts, message_id):
        """Recursively extract attachments from message parts"""
        for part in parts:
            # Check if this part has subparts
            if 'parts' in part:
                extract_attachments(part['parts'], message_id)
            
            # Check if this part is an attachment
            filename = part.get('filename', '')
            if filename:
                # Get file extension
                file_ext = Path(filename).suffix.lower()
                
                # Check if extension is allowed
                if file_ext not in ATTACHMENT_CONFIG['allowed_extensions']:
                    print(f"      ⏭️  Skipped (type not allowed): {filename}")
                    continue
                
                # Get attachment size
                attachment_id = part['body'].get('attachmentId')
                size_bytes = part['body'].get('size', 0)
                size_mb = size_bytes / (1024 * 1024)
                
                # Check size limit
                if size_mb > ATTACHMENT_CONFIG['max_file_size_mb']:
                    print(f"      ⏭️  Skipped (too large: {size_mb:.2f}MB): {filename}")
                    
                    if ATTACHMENT_CONFIG['save_metadata_only_if_too_large']:
                        attachment_info = {
                            'filename': filename,
                            'size_mb': size_mb,
                            'mime_type': part.get('mimeType', 'unknown'),
                            'attachment_id': attachment_id,
                            'skip_reason': f'Exceeds size limit ({ATTACHMENT_CONFIG["max_file_size_mb"]}MB)'
                        }
                        save_attachment_metadata(attachment_info, message_id)
                    
                    continue
                
                # Download attachment
                if attachment_id:
                    try:
                        attachment = service.users().messages().attachments().get(
                            userId='me',
                            messageId=message_id,
                            id=attachment_id
                        ).execute()
                        
                        file_data = base64.urlsafe_b64decode(attachment['data'])
                        
                        # Create email-specific folder for attachments
                        email_attachment_dir = ATTACHMENTS_DIR / f"email_{message_id}"
                        email_attachment_dir.mkdir(parents=True, exist_ok=True)
                        
                        # Sanitize filename
                        safe_filename = "".join(c for c in filename if c.isalnum() or c in (' ', '.', '_', '-')).strip()
                        attachment_path = email_attachment_dir / safe_filename
                        
                        # Save attachment
                        with open(attachment_path, 'wb') as f:
                            f.write(file_data)
                        
                        # Special handling for PDFs - check page count
                        skip_pdf = False
                        if file_ext == '.pdf' and ATTACHMENT_CONFIG['pdf_max_pages']:
                            page_count = get_pdf_page_count(attachment_path)
                            if page_count and page_count > ATTACHMENT_CONFIG['pdf_max_pages']:
                                print(f"      📄 PDF too long ({page_count} pages): {safe_filename}")
                                
                                if ATTACHMENT_CONFIG['save_metadata_only_if_too_large']:
                                    attachment_info = {
                                        'filename': safe_filename,
                                        'size_mb': size_mb,
                                        'mime_type': part.get('mimeType', 'unknown'),
                                        'attachment_id': attachment_id,
                                        'skip_reason': f'PDF has {page_count} pages (limit: {ATTACHMENT_CONFIG["pdf_max_pages"]})'
                                    }
                                    save_attachment_metadata(attachment_info, message_id)
                                
                                # Delete the downloaded file
                                attachment_path.unlink()
                                skip_pdf = True
                        
                        if not skip_pdf:
                            # ===== NEW: EXTRACT TEXT IMMEDIATELY! =====
                            extracted_text = None
                            if ATTACHMENT_CONFIG['extract_text_immediately']:
                                if file_ext == '.pdf':
                                    extracted_text = extract_pdf_text(attachment_path)
                                elif file_ext == '.docx':
                                    extracted_text = extract_docx_text(attachment_path)
                                elif file_ext == '.csv':
                                    extracted_text = extract_csv_text(attachment_path)
                                elif file_ext in ['.xlsx', '.xls']:
                                    extracted_text = extract_excel_text(attachment_path)
                                elif file_ext == '.txt':
                                    extracted_text = extract_text_file(attachment_path)
                                elif file_ext in ['.jpg', '.jpeg', '.png']:
                                    extracted_text = extract_image_text(attachment_path)
                                
                                # Save extracted text if successful
                                if extracted_text:
                                    output_filename = f"attachment_{message_id}_{Path(safe_filename).stem}.txt"
                                    output_path = PROCESSED_DIR / output_filename
                                    
                                    content = f"""Attachment from Email ID: {message_id}
Original Filename: {safe_filename}
File Type: {file_ext}
File Path: {attachment_path}

--- Extracted Content ---

{extracted_text}
"""
                                    
                                    with open(output_path, 'w', encoding='utf-8') as f:
                                        f.write(content)
                                    
                                    print(f"      ✅ Extracted text: {output_filename}")
                            
                            attachments_info.append({
                                'filename': safe_filename,
                                'path': str(attachment_path),
                                'size_mb': size_mb,
                                'type': file_ext,
                                'text_extracted': extracted_text is not None
                            })
                            print(f"      📎 Downloaded ({size_mb:.2f}MB): {safe_filename}")
                    
                    except Exception as e:
                        print(f"      ⚠️  Error downloading {filename}: {e}")
    
    # Start extraction
    if 'parts' in payload:
        extract_attachments(payload['parts'], message_id)
    
    return attachments_info


def fetch_new_emails(service, synced_ids, search_query="", max_check=50):
    """Fetch only new emails that haven't been synced yet"""
    
    try:
        # Get recent messages
        results = service.users().messages().list(
            userId='me',
            maxResults=max_check,
            q=search_query
        ).execute()
        
        messages = results.get('messages', [])
        
        if not messages:
            return 0
        
        # Filter out already-synced emails
        new_messages = [msg for msg in messages if msg['id'] not in synced_ids]
        
        if not new_messages:
            return 0
        
        print(f"📥 Found {len(new_messages)} new emails")
        
        saved_count = 0
        
        for message in new_messages:
            try:
                # Save to file with consistent naming
                filename = f"email_{message['id']}.txt"
                filepath = OUTPUT_DIR / filename
                
                # DUPLICATE CHECK: Skip if file already exists
                if filepath.exists():
                    print(f"  ⏭️  Skipped: {filename} (already exists)")
                    save_synced_id(message['id'])
                    synced_ids.add(message['id'])
                    continue
                
                # Get full message
                msg = service.users().messages().get(
                    userId='me',
                    id=message['id'],
                    format='full'
                ).execute()
                
                # Extract headers
                headers = msg['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown')
                date = next((h['value'] for h in headers if h['name'] == 'Date'), '')
                to = next((h['value'] for h in headers if h['name'] == 'To'), '')
                
                # Get body
                body = get_email_body(msg['payload'])
                
                if not body:
                    # Skip emails with no text body
                    save_synced_id(message['id'])
                    continue
                
                # Process attachments
                print(f"  📧 Processing: {subject[:50]}...")
                attachments = process_attachments(service, message['id'], msg['payload'], subject)
                
                # Create email content with attachment info
                email_content = f"""From: {sender}
To: {to}
Subject: {subject}
Date: {date}
Message-ID: {message['id']}
Attachments: {len(attachments)}

{body}
"""
                
                # Add attachment references
                if attachments:
                    email_content += "\n\n--- Attachments ---\n"
                    for att in attachments:
                        email_content += f"- {att['filename']} ({att['size_mb']:.2f}MB) - Path: {att['path']}\n"
                
                # Save to file (atomic write to prevent corruption)
                temp_filepath = filepath.with_suffix('.tmp')
                with open(temp_filepath, 'w', encoding='utf-8') as f:
                    f.write(email_content)
                
                # Atomic rename (prevents partial files)
                temp_filepath.rename(filepath)
                
                # Track this email
                save_synced_id(message['id'])
                synced_ids.add(message['id'])
                
                saved_count += 1
                attachment_info = f" ({len(attachments)} attachments)" if attachments else ""
                print(f"  ✅ Saved: {subject[:50]}...{attachment_info}")
            
            except Exception as e:
                print(f"  ⚠️  Error processing {message['id']}: {e}")
                continue
        
        return saved_count
    
    except HttpError as error:
        print(f'❌ Gmail API error: {error}')
        return 0


def watch_gmail(service, check_interval=60, search_query="newer_than:7d"):
    """Continuously watch for new emails
    
    Args:
        service: Gmail API service
        check_interval: Seconds between checks (default: 60)
        search_query: Gmail query to filter emails (default: last 7 days)
    """
    
    print("=" * 70)
    print("          Gmail Watcher - Real-time Sync with Attachments")
    print("=" * 70)
    print(f"✓ Monitoring your Gmail inbox")
    print(f"✓ Check interval: {check_interval} seconds")
    print(f"✓ Search query: '{search_query if search_query else 'All emails'}'")
    print(f"✓ Output directory: {OUTPUT_DIR}")
    print(f"✓ Attachments directory: {ATTACHMENTS_DIR}")
    print(f"✓ Processed text directory: {PROCESSED_DIR}")
    print(f"✓ Attachment processing: {'ENABLED' if ATTACHMENT_CONFIG['enabled'] else 'DISABLED'}")
    if ATTACHMENT_CONFIG['enabled']:
        print(f"  - Max file size: {ATTACHMENT_CONFIG['max_file_size_mb']}MB")
        print(f"  - Allowed types: {', '.join(ATTACHMENT_CONFIG['allowed_extensions'])}")
        print(f"  - PDF page limit: {ATTACHMENT_CONFIG['pdf_max_pages'] or 'None'}")
        print(f"  - 🚀 INSTANT text extraction: {ATTACHMENT_CONFIG['extract_text_immediately']}")
    print("\n⚠️  Keep this running alongside app.py for real-time updates")
    print("   Press Ctrl+C to stop\n")
    print("=" * 70)
    
    # Load already-synced email IDs
    synced_ids = load_synced_ids()
    print(f"\n📊 Already synced: {len(synced_ids)} emails")
    
    check_count = 0
    
    try:
        while True:
            check_count += 1
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            print(f"\n[{timestamp}] Check #{check_count} - Looking for new emails...")
            
            # Fetch new emails
            new_count = fetch_new_emails(
                service, 
                synced_ids, 
                search_query=search_query,
                max_check=50  # Check last 50 emails each time
            )
            
            if new_count > 0:
                print(f"✅ Downloaded {new_count} new emails")
                print(f"   Pathway will automatically index them!")
            else:
                print("   No new emails")
            
            # Wait before next check
            print(f"   Next check in {check_interval} seconds...")
            time.sleep(check_interval)
    
    except KeyboardInterrupt:
        print("\n\n👋 Stopping Gmail watcher...")
        print(f"✅ Total emails synced: {len(synced_ids)}")
        print("=" * 70)


def initial_sync(service, max_emails=100, search_query="newer_than:30d"):
    """Do initial sync of recent emails"""
    
    print("\n🔄 Running initial sync...")
    print(f"   Fetching up to {max_emails} emails")
    print(f"   Query: '{search_query}'")
    
    synced_ids = load_synced_ids()
    new_count = fetch_new_emails(
        service,
        synced_ids,
        search_query=search_query,
        max_check=max_emails
    )
    
    print(f"\n✅ Initial sync complete: {new_count} new emails downloaded")
    
    return synced_ids


def main():
    """Main function"""
    
    # ===== CONFIGURE HERE =====
    CHECK_INTERVAL = 60        # Check every 60 seconds (1 minute)
    SEARCH_QUERY = "newer_than:7d"  # Only sync emails from last 7 days
    INITIAL_SYNC_MAX = 100     # Max emails to fetch on first run
    # ==========================
    
    print("\n📧 Gmail Watcher Configuration:")
    print(f"   Check interval: {CHECK_INTERVAL} seconds")
    print(f"   Search query: '{SEARCH_QUERY}'")
    print(f"   Initial sync max: {INITIAL_SYNC_MAX} emails")
    
    # Authenticate
    print("\n🔐 Authenticating with Gmail...")
    creds = authenticate_gmail()
    
    if not creds:
        return
    
    service = build('gmail', 'v1', credentials=creds)
    print("✅ Authentication successful!")
    
    # Do initial sync
    synced_ids = initial_sync(service, INITIAL_SYNC_MAX, SEARCH_QUERY)
    
    # Start watching
    print("\n" + "=" * 70)
    input("Press Enter to start real-time monitoring (or Ctrl+C to exit)...")
    
    watch_gmail(service, check_interval=CHECK_INTERVAL, search_query=SEARCH_QUERY)


if __name__ == "__main__":
    main()