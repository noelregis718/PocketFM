# Fast Llama Classifier Setup & Execution Guide

This comprehensive guide covers everything you need to know to set up, configure, and run the `fast_llama_classifier.py` script from scratch. The script uses a local AI model (`Qwen 2.5`) to automatically classify book genres based on their descriptions and categories in an Excel spreadsheet.

---

## Part 1: System Requirements & Installation

### 1. Python Environment
Ensure you have **Python 3.8 or higher** installed. 
Install the required Python libraries by opening your terminal (Command Prompt or PowerShell) and running:
```powershell
pip install pandas requests openpyxl
```

### 2. Installing Ollama (The AI Engine)
The script relies on Ollama to run the AI model entirely locally on your machine (meaning no API costs and complete data privacy).
1. Navigate to [https://ollama.com](https://ollama.com) in your web browser.
2. Click **Download for Windows** (or select your OS).
3. Run the downloaded `OllamaSetup.exe` file and complete the installation.
4. **Crucial:** Ensure the Ollama app is running in the background. You should see its little llama icon in your Windows system tray (bottom right corner of your screen).

### 3. Downloading the Qwen Model
We use **Qwen 2.5 (1.5 Billion parameters)** because it is incredibly fast and highly accurate for strict JSON classification tasks.
1. Open a **new** PowerShell or Command Prompt window (so it recognizes the newly installed Ollama).
2. Run the following command to download the model:
   ```powershell
   ollama run qwen2.5:1.5b
   ```
3. The download is approximately 1 GB. Once it finishes, you will see a `>>>` prompt indicating the model is loaded and ready.
4. Type `/bye` and press Enter to exit the prompt.

---

## Part 2: Configuring the Script

Before running the script, open `backend/fast_llama_classifier.py` in your code editor and verify these two things:

### 1. The Excel File Path
Ensure the `EXCEL_FILE` variable points to the correct absolute path of the spreadsheet on your local machine.
```python
EXCEL_FILE = r"E:\Internship\PocketFM\Amazon A-Z Crawl List.xlsx"
SHEET_NAME = "Sheet1"
```

### 2. The Batch Limit (Scaling Up)
By default, the script might have a safety limit to only process a small batch of rows (e.g., 10 or 100 rows) at a time for testing.
* **To process the ENTIRE spreadsheet:** Find this line in the code:
  ```python
  rows_to_process = rows_to_process[:100]
  ```
  And either delete it or comment it out by adding a `#` in front of it:
  ```python
  # rows_to_process = rows_to_process[:100]
  ```

---

## Part 3: Running the Script

1. Open your terminal and navigate to the folder containing the script:
   ```powershell
   cd path/to/PocketFM/backend
   ```
2. Execute the script:
   ```powershell
   python fast_llama_classifier.py
   ```

### What to expect while it runs:
* **Speed:** The script sends 10 concurrent requests to the model at once (`MAX_WORKERS = 10`). Depending on your CPU/GPU, it will process roughly 2-10 books per second.
* **Safety:** It automatically saves a checkpoint to the Excel file every 50 rows. 
* **Resuming:** If you stop the script halfway through, don't worry! The next time you run it, it automatically skips rows that already have a genre and picks up exactly where it left off.
* **Output:** It will create/populate two new columns directly in your Excel sheet: `Detailed Genre (AI)` (Fantasy, Romantasy, or Romance Drama) and `AI Reasoning`.

---

## Troubleshooting

* **Error: "Ollama API Failure" in the Excel sheet** 
  * *Fix:* This means the Python script cannot connect to the local Ollama server. Ensure the Ollama application is actually running on your computer (check your system tray for the icon).
* **Error: "No such file or directory" when running python**
  * *Fix:* You are in the wrong folder in your terminal. Use `cd` to navigate into the `backend` folder before running `python fast_llama_classifier.py`.
