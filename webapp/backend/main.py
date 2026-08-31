import os
import uuid
import threading
from typing import List
from fastapi import FastAPI, UploadFile, File, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from converter import process_job_sync, get_progress

app = FastAPI(title="PDF to DOCX Converter")

# Allow frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
CONVERTED_DIR = "converted"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(CONVERTED_DIR, exist_ok=True)

@app.post("/api/upload")
async def upload_files(background_tasks: BackgroundTasks, files: List[UploadFile] = File(...)):
    job_id = str(uuid.uuid4())
    files_to_convert = []
    
    for file in files:
        file_id = str(uuid.uuid4())
        
        # Save uploaded PDF
        pdf_filename = f"{file_id}_{file.filename}"
        pdf_path = os.path.join(UPLOAD_DIR, pdf_filename)
        
        with open(pdf_path, "wb") as f:
            f.write(await file.read())
            
        # Determine output DOCX path
        docx_filename = file.filename.replace(".pdf", ".docx")
        if not docx_filename.endswith(".docx"):
            docx_filename += ".docx"
            
        docx_path = os.path.join(CONVERTED_DIR, f"{file_id}_{docx_filename}")
        
        files_to_convert.append((pdf_path, docx_path, file_id))
        
    # Start conversion in a background thread
    thread = threading.Thread(target=process_job_sync, args=(job_id, files_to_convert))
    thread.start()
    
    # Return file mapping to frontend so it can track individual files
    file_mapping = [{"file_id": f_id, "filename": f.filename} for f, (_, _, f_id) in zip(files, files_to_convert)]
    return {"job_id": job_id, "files": file_mapping}

@app.get("/api/status/{job_id}")
def check_status(job_id: str):
    return get_progress(job_id)

@app.get("/api/download/{file_id}")
def download_file(file_id: str):
    # Find the converted file starting with file_id
    for filename in os.listdir(CONVERTED_DIR):
        if filename.startswith(file_id):
            path = os.path.join(CONVERTED_DIR, filename)
            # Send file as attachment so it forces download with original filename
            original_name = filename.replace(f"{file_id}_", "")
            return FileResponse(path=path, filename=original_name)
            
    return {"error": "File not found or conversion incomplete."}
