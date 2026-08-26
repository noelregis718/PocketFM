from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class BookDownloadTask:
    title: str
    number: int
    status: str = "pending" # pending, downloaded, converted, failed
    pdf_path: Optional[str] = None
    docx_path: Optional[str] = None
    error_message: Optional[str] = None

@dataclass
class SeriesTask:
    name: str
    goodreads_link: str
    books: List[BookDownloadTask] = field(default_factory=list)
    status: str = "pending" # pending, scraping_gr, downloading, done, failed
