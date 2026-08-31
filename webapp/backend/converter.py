import os
import re
import logging
from pdf2docx import Converter

# Store progress mapping: job_id -> { "file_id": progress_int }
progress_store = {}
jobs_status = {}

def update_progress(job_id: str, file_id: str, progress: int):
    if job_id not in progress_store:
        progress_store[job_id] = {}
    progress_store[job_id][file_id] = progress

def get_progress(job_id: str):
    return {
        "status": jobs_status.get(job_id, "unknown"),
        "files": progress_store.get(job_id, {})
    }

class ProgressHandler(logging.Handler):
    """Intercepts logging output to parse pdf2docx progress"""
    def __init__(self, job_id, file_id):
        super().__init__()
        self.job_id = job_id
        self.file_id = file_id
        
    def emit(self, record):
        message = self.format(record)
        # Parse output like: "[INFO] (45/216) Page 45" or "(45/216)"
        match_page = re.search(r'\(\s*(\d+)\s*/\s*(\d+)\s*\)', message)
        match_step = re.search(r'\[(\d+)/4\]', message)
        
        if match_page:
            current = int(match_page.group(1))
            total = int(match_page.group(2))
            if total > 0:
                # 10% to 95% is for pages
                percentage = 10 + int((current / total) * 85)
                update_progress(self.job_id, self.file_id, percentage)
        elif match_step:
            step = int(match_step.group(1))
            if step == 1:
                update_progress(self.job_id, self.file_id, 2)
            elif step == 2:
                update_progress(self.job_id, self.file_id, 5)
            elif step == 3:
                update_progress(self.job_id, self.file_id, 10)

def convert_pdf_to_docx_with_progress(pdf_path: str, docx_path: str, job_id: str, file_id: str):
    try:
        update_progress(job_id, file_id, 2) # Start processing
        
        # Attach our custom handler to the root logger to catch pdf2docx output
        handler = ProgressHandler(job_id, file_id)
        logging.getLogger().addHandler(handler)
        
        cv = Converter(pdf_path)
        try:
            cv.convert(docx_path)
        finally:
            cv.close()
            logging.getLogger().removeHandler(handler)
            
        update_progress(job_id, file_id, 100)
    except Exception as e:
        print(f"Error converting {pdf_path}: {e}")
        update_progress(job_id, file_id, -1) # -1 indicates error
        # cleanup handler on error
        try:
            logging.getLogger().removeHandler(handler)
        except:
            pass

def process_job_sync(job_id: str, files_to_convert: list):
    """
    files_to_convert: list of tuples (pdf_path, docx_path, file_id)
    """
    jobs_status[job_id] = "processing"
    
    # Initialize all files as queued (0% progress)
    for _, _, file_id in files_to_convert:
        update_progress(job_id, file_id, 0)
    
    for pdf_path, docx_path, file_id in files_to_convert:
        if jobs_status.get(job_id) == "cancelled":
            break
        convert_pdf_to_docx_with_progress(pdf_path, docx_path, job_id, file_id)
        
    if jobs_status.get(job_id) != "cancelled":
        jobs_status[job_id] = "completed"

