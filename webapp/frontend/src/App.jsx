import React, { useState, useEffect, useCallback, useRef } from 'react';
import { UploadCloud, FileText, CheckCircle, Download, File as FileIcon } from 'lucide-react';

const API_BASE = 'http://localhost:8000/api';

function App() {
  const [isDragging, setIsDragging] = useState(false);
  const [jobId, setJobId] = useState(null);
  const [files, setFiles] = useState([]);
  const pollIntervalRef = useRef(null);

  const handleDragOver = (e) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    setIsDragging(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDragging(false);
    const droppedFiles = Array.from(e.dataTransfer.files).filter(f => f.type === 'application/pdf' || f.name.endsWith('.pdf'));
    if (droppedFiles.length > 0) {
      uploadFiles(droppedFiles);
    }
  };

  const handleFileInput = (e) => {
    const selectedFiles = Array.from(e.target.files).filter(f => f.type === 'application/pdf' || f.name.endsWith('.pdf'));
    if (selectedFiles.length > 0) {
      uploadFiles(selectedFiles);
    }
  };

  const uploadFiles = async (filesToUpload) => {
    const formData = new FormData();
    filesToUpload.forEach(file => {
      formData.append('files', file);
    });

    try {
      const res = await fetch(`${API_BASE}/upload`, {
        method: 'POST',
        body: formData,
      });
      const data = await res.json();
      
      const newFiles = data.files.map(f => ({
        id: f.file_id,
        name: f.filename,
        progress: 0,
        status: 'processing'
      }));
      
      setFiles(prev => [...prev, ...newFiles]);
      setJobId(data.job_id);
    } catch (err) {
      console.error("Upload failed", err);
      alert("Failed to upload files. Make sure the backend is running.");
    }
  };

  const pollStatus = useCallback(async () => {
    if (!jobId) return;
    
    try {
      const res = await fetch(`${API_BASE}/status/${jobId}`);
      const data = await res.json();
      
      setFiles(prevFiles => prevFiles.map(file => {
        const p = data.files[file.id];
        if (p !== undefined) {
          return {
            ...file,
            progress: p,
            status: p === 100 ? 'completed' : p === -1 ? 'error' : 'processing'
          };
        }
        return file;
      }));
      
      if (data.status === 'completed' || data.status === 'cancelled') {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
        setJobId(null); // Allow new uploads to track separately if needed
      }
    } catch (err) {
      console.error("Failed to poll status", err);
    }
  }, [jobId]);

  useEffect(() => {
    if (jobId) {
      if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);
      pollIntervalRef.current = setInterval(pollStatus, 1000);
    }
    return () => {
      if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);
    };
  }, [jobId, pollStatus]);

  return (
    <>
      <div className="bg-orb orb-1"></div>
      <div className="bg-orb orb-2"></div>
      
      <div className="app-container">
        <div className="glass-panel">
          
          <div className="header">
            <h1>PocketFM</h1>
            <p>PDF to Doc Converter</p>
          </div>

          <label 
            className={`dropzone ${isDragging ? 'active' : ''}`}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
          >
            <input 
              type="file" 
              multiple 
              accept=".pdf,application/pdf" 
              onChange={handleFileInput}
              style={{ display: 'none' }}
            />
            <UploadCloud className="upload-icon" />
            <h3>Drag & Drop your PDFs here</h3>
            <p style={{ marginTop: '0.5rem', color: 'var(--text-secondary)' }}>or click to browse files</p>
          </label>

          {files.length > 0 && (
            <div className="file-list">
              {files.map((file, idx) => (
                <div className="file-item" key={`${file.id}-${idx}`}>
                  <div className="file-info">
                    <div className="file-name">
                      {file.status === 'completed' ? (
                        <FileText size={20} color="var(--success-color)" />
                      ) : (
                        <FileIcon size={20} color="var(--text-secondary)" />
                      )}
                      <span>{file.name}</span>
                    </div>
                    <div className={`file-status ${file.status === 'completed' ? 'status-completed' : ''}`}>
                      {file.status === 'processing' && `${file.progress}%`}
                      {file.status === 'completed' && 'Ready'}
                      {file.status === 'error' && 'Failed'}
                    </div>
                  </div>
                  
                  <div className="progress-container">
                    <div 
                      className="progress-bar" 
                      style={{ 
                        width: `${Math.max(0, file.progress)}%`,
                        background: file.status === 'error' ? '#ef4444' : file.status === 'completed' ? 'var(--success-color)' : ''
                      }}
                    ></div>
                  </div>
                  
                  {file.status === 'completed' && (
                    <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '0.5rem' }}>
                      <a href={`${API_BASE}/download/${file.id}`} className="btn btn-success" download>
                        <Download size={16} /> Download DOCX
                      </a>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}

        </div>
      </div>
    </>
  );
}

export default App;
