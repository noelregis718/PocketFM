import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import { Search, Download, Trash2, BookOpen, Terminal, Sparkles, Database } from 'lucide-react';
import { PromptInputBox } from './components/ui/ai-prompt-box';

interface BookResult {
  Rank: string;
  'Book Title': string;
  'Author Name': string;
  Rating: number;
  Price: string;
  Publisher: string;
  'Amazon URL': string;
}

function App() {
  const [url, setUrl] = useState('https://www.amazon.com/best-sellers-books-Amazon/zgbs/books/');
  const [limit, setLimit] = useState(50);
  const [results, setResults] = useState<BookResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const logEndRef = useRef<HTMLDivElement>(null);

  const addLog = (msg: string) => {
    setLogs(prev => [...prev, `[${new Date().toLocaleTimeString()}] ${msg}`]);
  };

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  const handleScrape = async (inputValue?: string) => {
    const targetUrl = inputValue || url;
    if (!targetUrl) return alert('Please enter a Bestseller URL');

    setLoading(true);
    setResults([]);
    setLogs([]);
    addLog('Searching for Amazon Bestsellers...');
    addLog(`Target: ${targetUrl}`);

    try {
      const response = await axios.post('http://localhost:5000/api/scrape-bestsellers', { 
        url: targetUrl, 
        limit: limit 
      });
      setResults(response.data.results);
      addLog(`Success! Extracted ${response.data.results.length} books.`);
      addLog('Deep extraction and text cleaning complete.');
      
      // Automatically trigger download
      addLog('Preparing Excel dataset for automatic download...');
      window.open('http://localhost:5000/api/download', '_blank');
      addLog('Download triggered.');
    } catch (error) {
      console.error('Scraping failed:', error);
      addLog('Error: Connection to backend failed.');
    } finally {
      setLoading(false);
    }
  };

  const handleDownload = () => {
    window.open('http://localhost:5000/api/download', '_blank');
    addLog('Excel dataset downloaded successfully.');
  };

  return (
    <div className="flex h-screen bg-background text-foreground">

      {/* Main Content */}
      <main className="flex-1 flex flex-col items-center overflow-y-auto px-6 py-12 md:py-20 bg-[radial-gradient(circle_at_top,rgba(56,189,248,0.05)_0%,transparent_50%)]">
        
        {/* Header */}
        <header className="text-center mb-12 max-w-2xl mt-30">
          <h1 className="text-4xl md:text-5xl font-bold tracking-tight mb-4 bg-gradient-to-r from-primary to-indigo-400 bg-clip-text text-transparent">
            Amazon Bestseller Scraper
          </h1>
          <p className="text-lg text-muted-foreground">
            Paste a bestseller URL below to trigger deep-link extraction and automated data cleaning.
          </p>
        </header>

        {/* Input Section */}
        <div className="w-full max-w-3xl mb-8">
          <PromptInputBox 
            onSend={(msg) => handleScrape(msg)}
            isLoading={loading}
            placeholder="Paste Amazon Bestseller URL (e.g., https://www.amazon.com/zgbs/books/)"
          />
          <div className="flex justify-center mt-3 gap-4 text-xs text-muted-foreground">
              <div className="flex items-center gap-2">
                <span>Concurrent Tabs:</span>
                <span className="text-primary font-mono">15</span>
              </div>
              <div className="flex items-center gap-2 border-l border-border pl-4">
                <span>Row Limit:</span>
                <input 
                  type="number" 
                  value={limit} 
                  onChange={(e) => setLimit(parseInt(e.target.value))}
                  className="bg-transparent border border-white/10 rounded px-1 w-12 text-primary focus:outline-none"
                />
              </div>
          </div>
        </div>

        {/* Console / Logs */}
        {logs.length > 0 && (
          <div className="w-full max-w-3xl glass-panel rounded-2xl overflow-hidden mb-8 border-white/5 bg-black/60 shadow-inner">
            <div className="bg-white/5 px-4 py-2 border-bottom border-border flex items-center gap-2 text-[10px] text-muted-foreground font-mono uppercase tracking-widest">
              <Terminal className="w-3 h-3" />
              <span>System Execution Logs</span>
            </div>
            <div className="p-4 font-mono text-[13px] max-h-[180px] overflow-y-auto terminal-body">
              {logs.map((log, i) => (
                <div key={i} className="mb-1.5 flex gap-3">
                  <span className="text-primary/50 shrink-0">{log.substring(0, 10)}</span>
                  <span className="text-primary/90">{log.substring(10)}</span>
                </div>
              ))}
              <div ref={logEndRef} />
            </div>
          </div>
        )}

        {/* Results Grid */}
        {results.length > 0 && !loading && (
          <div className="w-full max-w-4xl glass-panel rounded-3xl p-6 border-white/10 bg-white/[0.02]">
            <div className="flex flex-col md:flex-row justify-between items-center gap-4 mb-8">
              <div>
                <h3 className="text-xl font-bold">
                  Extracted Dataset
                </h3>
                <p className="text-sm text-muted-foreground">Successfully parsed {results.length} items from the source list.</p>
              </div>
              <button 
                onClick={handleDownload}
                className="flex items-center gap-2 bg-primary text-primary-foreground px-5 py-2.5 rounded-xl font-bold hover:scale-105 transition-all shadow-lg active:scale-95 whitespace-nowrap"
              >
                <Download className="w-5 h-5" />
                Download Excel (Formatted)
              </button>
            </div>

            <div className="overflow-x-auto rounded-xl border border-white/5">
              <table className="w-full text-left text-sm">
                <thead>
                  <tr className="bg-white/5 text-muted-foreground uppercase text-[10px] tracking-widest">
                    <th className="p-4">Rank</th>
                    <th className="p-4">Book Details</th>
                    <th className="p-4">Rating</th>
                    <th className="p-4">Price</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-white/5">
                  {results.slice(0, 6).map((book, i) => (
                    <tr key={i} className="hover:bg-white/[0.03] transition-colors">
                      <td className="p-4 font-mono text-primary font-bold">#{book.Rank}</td>
                      <td className="p-4 max-w-[400px]">
                        <div className="font-bold text-foreground mb-0.5 line-clamp-1">{book['Book Title']}</div>
                        <div className="text-xs text-muted-foreground truncate">{book['Author Name']}</div>
                        <div className="text-[10px] text-muted-foreground/60 italic mt-1">{book.Publisher || 'Deep indexing in progress...'}</div>
                      </td>
                      <td className="p-4">
                        <div className="flex items-center gap-1.5">
                          <span className="bg-primary/20 text-primary px-2 py-0.5 rounded text-xs font-bold">{book.Rating}</span>
                        </div>
                      </td>
                      <td className="p-4 font-bold text-primary">{book.Price}</td>
                    </tr>
                  ))}
                  {results.length > 6 && (
                    <tr>
                      <td colSpan={4} className="p-6 text-center text-muted-foreground italic bg-white/[0.01]">
                        ... and {results.length - 6} more high-quality entries available in the downloaded Excel sheet.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
