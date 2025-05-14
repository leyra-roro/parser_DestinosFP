import pdfplumber
import pandas as pd
import os
import re

def extract_tables_from_pdf(pdf_file, excel_file):
    all_tables = []
    current_table_title = None
    current_table_headers = None
    current_table_rows = []
    
    # Define the expected column order - helps with alignment issues
    expected_columns = [
        "PUESTO NUMERO", 
        "CENTRO DIRECTIVO/00.A.A\nCENTRO DE DESTINO",
        "PROVINCIA\nLOCALIDAD",
        "PUESTO DE TRABAJO",
        "NIVEL C.D.\nC. ESPECIFICO"
    ]
    
    with pdfplumber.open(pdf_file) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            print(f"Processing page {page_num} of {len(pdf.pages)}")
            
            # Extract tables from the current page
            tables = page.extract_tables()
            
            if not tables:
                print(f"No tables found on page {page_num}")
                continue
            
            for table_num, table in enumerate(tables, 1):
                print(f"  - Found table #{table_num} on page {page_num} with {len(table)} rows")
                
                # Try to identify if this is a new table or continuation
                if len(table) > 0 and any("PUESTO" in str(cell) for cell in table[0]):
                    # This looks like a header row, so it's likely a new table
                    # If we were collecting data for a previous table, save it first
                    if current_table_headers and current_table_rows:
                        table_df = pd.DataFrame(current_table_rows, columns=current_table_headers)
                        if current_table_title:
                            table_df.insert(0, "ORGANISMO", current_table_title)
                        all_tables.append(table_df)
                    
                    # Look for a title above the table
                    text = page.extract_text()
                    if text:
                        lines = text.split('\n')
                        for i, line in enumerate(lines):
                            if "PUESTO" in line and any(header in line for header in ["CENTRO", "UBICACIÓN"]):
                                if i > 0:
                                    current_table_title = lines[i-1].strip()
                                    print(f"  - Found title: {current_table_title}")
                                break
                    
                    # Start a new table
                    if table[0] and all(cell is not None for cell in table[0]):
                        # Don't modify the original headers at this stage
                        current_table_headers = [str(cell).strip() for cell in table[0]]
                        
                        # Verify column order matches expected pattern
                        for i, expected_col in enumerate(expected_columns):
                            if i < len(current_table_headers):
                                if expected_col not in current_table_headers[i]:
                                    print(f"Warning: Column {i} doesn't match expected pattern. Got: {current_table_headers[i]}, Expected: {expected_col}")
                        
                        current_table_rows = []
                        
                        # Add data rows from this table - KEEP newlines for now
                        for row in table[1:]:
                            if row and all(cell is not None for cell in row) and len(row) == len(current_table_headers):
                                processed_row = []
                                for i, cell in enumerate(row):
                                    cell_text = str(cell).strip()
                                    
                                    # Special handling for "PUESTO NUMERO" column
                                    if i == 0 and not re.match(r"^\d+$", cell_text):
                                        # This doesn't look like a position number, might be misaligned
                                        print(f"Warning: Possible misalignment in row. First cell: {cell_text}")
                                        # Try to fix by shifting cells if needed
                                        if len(row) >= 2 and re.match(r"^\d+$", str(row[1]).strip()):
                                            # Second column has the position number, shift right
                                            print(f"  - Fixing alignment, shifting right")
                                            # [empty, position_num, description, location, level]
                                            processed_row = [""] + [str(c).strip() for c in row[:-1]]
                                            continue
                                    
                                    processed_row.append(cell_text)
                                
                                # Ensure the row has the right number of columns
                                while len(processed_row) < len(current_table_headers):
                                    processed_row.append("")
                                
                                current_table_rows.append(processed_row[:len(current_table_headers)])
                else:
                    # This is likely a continuation of the previous table
                    if current_table_headers:
                        for row in table:
                            if row and all(cell is not None for cell in row) and len(row) == len(current_table_headers):
                                processed_row = []
                                for i, cell in enumerate(row):
                                    cell_text = str(cell).strip()
                                    
                                    # Special handling for "PUESTO NUMERO" column
                                    if i == 0 and not re.match(r"^\d+$", cell_text):
                                        # This doesn't look like a position number, might be misaligned
                                        print(f"Warning: Possible misalignment in row. First cell: {cell_text}")
                                        # Try to fix by shifting cells if needed
                                        if len(row) >= 2 and re.match(r"^\d+$", str(row[1]).strip()):
                                            # Second column has the position number, shift right
                                            print(f"  - Fixing alignment, shifting right")
                                            processed_row = [""] + [str(c).strip() for c in row[:-1]]
                                            continue
                                    
                                    processed_row.append(cell_text)
                                
                                # Ensure the row has the right number of columns
                                while len(processed_row) < len(current_table_headers):
                                    processed_row.append("")
                                    
                                current_table_rows.append(processed_row[:len(current_table_headers)])
    
    # Don't forget to add the last table
    if current_table_headers and current_table_rows:
        table_df = pd.DataFrame(current_table_rows, columns=current_table_headers)
        if current_table_title:
            table_df.insert(0, "ORGANISMO", current_table_title)
        all_tables.append(table_df)
    
    # Process all tables to split columns
    processed_tables = []
    for table_df in all_tables:
        # Find and split the "PROVINCIA LOCALIDAD" column
        province_locality_col = None
        for col in table_df.columns:
            if "PROVINCIA" in col and "LOCALIDAD" in col:
                province_locality_col = col
                break
        
        if province_locality_col:
            print(f"Splitting column: {province_locality_col}")
            # Create new columns
            table_df['PROVINCIA'] = table_df[province_locality_col].apply(
                lambda x: x.split('\n')[0].strip() if '\n' in x else x.strip())
            table_df['LOCALIDAD'] = table_df[province_locality_col].apply(
                lambda x: x.split('\n')[1].strip() if '\n' in x and len(x.split('\n')) > 1 else "")
            # Drop the original column
            table_df = table_df.drop(columns=[province_locality_col])
        
        # Find and split the "NIVEL C.D. C. ESPECÍFICO" column
        nivel_col = None
        for col in table_df.columns:
            if "NIVEL" in col and ("ESPECÍFICO" in col or "ESPECIFICO" in col):
                nivel_col = col
                break
        
        if nivel_col:
            print(f"Splitting column: {nivel_col}")
            # Create new columns
            table_df['NIVEL C.D.'] = table_df[nivel_col].apply(
                lambda x: x.split('\n')[0].strip() if '\n' in x else x.strip())
            table_df['C. ESPECÍFICO'] = table_df[nivel_col].apply(
                lambda x: x.split('\n')[1].strip() if '\n' in x and len(x.split('\n')) > 1 else "")
            # Drop the original column
            table_df = table_df.drop(columns=[nivel_col])
            
        # Clean up any remaining newlines in other columns
        for col in table_df.columns:
            table_df[col] = table_df[col].str.replace('\n', ' ')
            
        processed_tables.append(table_df)
    
    # Combine all tables into one Excel file with multiple sheets
    with pd.ExcelWriter(excel_file) as writer:
        if processed_tables:
            # First, save all tables combined into one sheet
            combined_df = pd.concat(processed_tables, ignore_index=True)
            combined_df.to_excel(writer, sheet_name="All Tables", index=False)
            
            # Then save each table to its own sheet
            for i, table_df in enumerate(processed_tables, 1):
                sheet_name = f"Table {i}"
                if "ORGANISMO" in table_df.columns and not table_df["ORGANISMO"].isna().all():
                    title = table_df["ORGANISMO"].iloc[0]
                    # Excel sheet names must be <= 31 chars
                    sheet_name = title[:25] if len(title) > 25 else title
                table_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"✅ Extracted {len(processed_tables)} tables to: {excel_file}")
        else:
            print("❌ No tables were extracted from the PDF")

if __name__ == "__main__":
    pdf_file = "Listadodevacantes.pdf.pdf"
    excel_file = "output.xlsx"
    
    # Check if PDF exists
    if not os.path.isfile(pdf_file):
        print(f"❌ File not found: {pdf_file}")
    else:
        extract_tables_from_pdf(pdf_file, excel_file)
