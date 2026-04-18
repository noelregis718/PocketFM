from fpdf import FPDF

def create_mock_pdf():
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    for i in range(1, 4):
        pdf.add_page()
        pdf.set_font("Arial", 'B', 16)
        pdf.cell(200, 10, f"Book Report: Sample Title {i}", ln=True, align='C')
        pdf.ln(10)
        
        pdf.set_font("Arial", size=12)
        pdf.cell(200, 10, "Amazon Metadata:", ln=True)
        pdf.cell(200, 10, "- Stars: 4.5", ln=True)
        pdf.cell(200, 10, "- Ratings: 1,200", ln=True)
        pdf.ln(5)
        
        pdf.cell(200, 10, "Goodreads Metadata:", ln=True)
        pdf.cell(200, 10, "- Series: Fantasy Romance Saga", ln=True)
        pdf.ln(5)
        
        pdf.cell(200, 10, "Author Contacts:", ln=True)
        pdf.cell(200, 10, "- Email: author@example.com", ln=True)

    pdf.output("test_report.pdf")
    print("PDF Generated: test_report.pdf")

if __name__ == '__main__':
    create_mock_pdf()
