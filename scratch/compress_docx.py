import zipfile
import io
import os
from PIL import Image

def compress_docx(input_path, output_path):
    print(f"Compressing {input_path}...")
    with zipfile.ZipFile(input_path, 'r') as zin:
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                
                # Compress images
                if item.filename.startswith('word/media/') and item.filename.lower().endswith(('.png', '.jpeg', '.jpg')):
                    try:
                        img = Image.open(io.BytesIO(data))
                        if img.mode != 'RGB':
                            img = img.convert('RGB')
                        
                        img.thumbnail((1200, 1200), Image.Resampling.LANCZOS)
                        
                        out_io = io.BytesIO()
                        img.save(out_io, format='JPEG', quality=40, optimize=True)
                        data = out_io.getvalue()
                        
                        if item.filename.lower().endswith('.png'):
                            item.filename = item.filename[:-4] + '.jpeg'
                            
                    except Exception as e:
                        print(f"Failed to compress {item.filename}: {e}")
                
                # Replace .png references with .jpeg in XML files
                elif item.filename.endswith('.xml') or item.filename.endswith('.rels'):
                    try:
                        text = data.decode('utf-8')
                        text = text.replace('.png"', '.jpeg"').replace('.PNG"', '.jpeg"')
                        
                        # In [Content_Types].xml, ensure jpeg is registered
                        if item.filename == '[Content_Types].xml':
                            if 'Extension="jpeg"' not in text:
                                text = text.replace('</Types>', '<Default Extension="jpeg" ContentType="image/jpeg"/></Types>')
                        
                        data = text.encode('utf-8')
                    except UnicodeDecodeError:
                        pass
                
                zout.writestr(item, data)
    print(f"Done! Saved to {output_path}")

input_file = "Book 6 - Murder with malice.docx"
output_file = "Book 6 - Murder with malice_compressed.docx"
compress_docx(input_file, output_file)
