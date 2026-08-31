from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class BookDownloadTask:
    title: str
    number: int
    author: str = ""
    status: str = "pending" # pending, downloaded, converted, failed
    source: Optional[str] = None # OceanOfPDF, Z-Library
    pdf_path: Optional[str] = None
    epub_path: Optional[str] = None
    docx_path: Optional[str] = None
    error_message: Optional[str] = None

@dataclass
class SeriesTask:
    name: str
    goodreads_link: str
    books: List[BookDownloadTask] = field(default_factory=list)
    status: str = "pending" # pending, scraping_gr, downloading, done, failed
