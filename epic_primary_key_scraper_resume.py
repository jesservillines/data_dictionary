import os
import time
import csv
import pandas as pd
from playwright.sync_api import sync_playwright
import re
from datetime import datetime
import sys

# Define the columns for our CSV
columns = ['table_name', 'column_name', 'is_primary_key', 'ordinal_position']

# Function to print with timestamp
def log(message):
    """Print a message with timestamp and immediately flush output"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()

# Function to extract primary key data from a table page
def extract_primary_key_data(page, table_name, url):
    try:
        log(f"Processing table: {table_name}")
        
        # Navigate with a timeout and wait until network is idle
        page.goto(url, timeout=60000, wait_until='networkidle')
        
        # Wait for page content to load
        page.wait_for_selector('table', timeout=10000)
        
        # Get all tables on the page
        tables = page.query_selector_all('table')
        log(f"Found {len(tables)} tables on the page")
        
        if not tables:
            log("No tables found on the page. Skipping.")
            return []
        
        primary_key_data = []
        
        # Extract primary key information
        primary_keys = []
        
        # Look for a table that has column names and ordinal positions
        # This is likely the primary key table
        for table in tables:
            header_row = table.query_selector('tr:first-child')
            if not header_row:
                continue
                
            header_cells = header_row.query_selector_all('td')
            header_text = ' '.join([cell.inner_text().strip() for cell in header_cells])
            
            if 'Column Name' in header_text and 'Ordinal Position' in header_text:
                log("Found primary key table")
                
                # Get all rows except the header
                pk_rows = table.query_selector_all('tr:not(:first-child)')
                
                for row in pk_rows:
                    cells = row.query_selector_all('td')
                    if len(cells) >= 2:  # Assuming at least column name and ordinal position
                        column_name = cells[0].inner_text().strip()
                        ordinal_position = ''
                        
                        # Try to extract ordinal position if available
                        position_col_index = -1
                        for i, cell in enumerate(header_cells):
                            if 'Ordinal Position' in cell.inner_text().strip():
                                position_col_index = i
                                break
                                
                        if position_col_index >= 0 and position_col_index < len(cells):
                            ordinal_position = cells[position_col_index].inner_text().strip()
                        
                        primary_key_data.append({
                            'table_name': table_name,
                            'column_name': column_name,
                            'is_primary_key': 'Y',
                            'ordinal_position': ordinal_position
                        })
                        
                        primary_keys.append(column_name)
                
                log(f"Primary keys: {primary_keys}")
                break
        
        # If no specific primary key table was found, try to extract from column information
        if not primary_key_data:
            log("No dedicated primary key table found. Checking column information.")
            
            # Find column information table - look for tables with numeric first columns
            column_table = None
            for table in tables:
                rows = table.query_selector_all('tr:not(:first-child)')
                if not rows:
                    continue
                    
                first_cell = rows[0].query_selector('td:first-child')
                if first_cell and first_cell.inner_text().strip().isdigit():
                    log("Found table with column information")
                    column_table = table
                    break
            
            if column_table:
                header_row = column_table.query_selector('tr:first-child')
                header_cells = header_row.query_selector_all('td')
                header_texts = [cell.inner_text().strip() for cell in header_cells]
                
                # Look for a PK indicator in the header
                pk_col_index = -1
                for i, text in enumerate(header_texts):
                    if 'Primary Key' in text or 'PK' in text:
                        pk_col_index = i
                        break
                
                # Process column information to find primary keys
                column_rows = column_table.query_selector_all('tr:not(:first-child)')
                
                for row in column_rows:
                    cells = row.query_selector_all('td')
                    
                    if len(cells) >= 4 and cells[0].inner_text().strip().isdigit():
                        position = cells[0].inner_text().strip()
                        column_name = cells[1].inner_text().strip()
                        
                        # Determine if this is a primary key
                        is_primary_key = 'N'
                        
                        # Method 1: Check for a primary key column
                        if pk_col_index >= 0 and pk_col_index < len(cells):
                            pk_text = cells[pk_col_index].inner_text().strip().upper()
                            if pk_text in ['Y', 'YES', 'TRUE', '1', 'PK']:
                                is_primary_key = 'Y'
                        
                        # Method 2: Check for PK indicator in the column name or description
                        if is_primary_key == 'N':
                            # Check in column name (common pattern is column name ending with _ID or containing PK)
                            if column_name.endswith('_ID') or 'KEY' in column_name.upper() or 'PK' in column_name.upper():
                                # This is a heuristic, might give false positives
                                is_primary_key = 'Y'  
                            
                            # Check in the row text for PK indicators
                            row_text = row.inner_text().upper()
                            if 'PRIMARY KEY' in row_text or 'PK:' in row_text or ' PK ' in row_text:
                                is_primary_key = 'Y'
                        
                        if is_primary_key == 'Y':
                            primary_key_data.append({
                                'table_name': table_name,
                                'column_name': column_name,
                                'is_primary_key': is_primary_key,
                                'ordinal_position': position
                            })
        
        log(f"Successfully extracted {len(primary_key_data)} primary key columns for table {table_name}")
        return primary_key_data
        
    except Exception as e:
        log(f"Error processing table {table_name}: {e}")
        return []

# Function to get already processed tables from summary file
def get_processed_tables(output_dir):
    processed_tables = set()
    summary_file = os.path.join(output_dir, 'pk_processing_summary.csv')
    
    if os.path.exists(summary_file):
        try:
            with open(summary_file, 'r', newline='') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                for row in reader:
                    if len(row) >= 2:
                        # Add the table name to processed tables
                        processed_tables.add(row[1])
            log(f"Found {len(processed_tables)} already processed tables")
        except Exception as e:
            log(f"Error reading summary file: {e}")
    
    return processed_tables

# Function to load progress
def load_progress(output_dir):
    progress_file = os.path.join(output_dir, 'pk_progress_state.csv')
    if os.path.exists(progress_file):
        try:
            df = pd.read_csv(progress_file)
            if 'letter' in df.columns:
                processed_letters = set(df['letter'].dropna().tolist())
                current_letter = None
                if 'current_letter' in df.columns and not df['current_letter'].isna().all():
                    current_letter = df['current_letter'].iloc[0]
                log(f"Loaded progress: processed letters: {sorted(processed_letters)}, current: {current_letter}")
                return processed_letters, current_letter
        except Exception as e:
            log(f"Error loading progress: {e}")
    return set(), None

# Function to save progress
def save_progress(output_dir, processed_letters, current_letter=None):
    progress_file = os.path.join(output_dir, 'pk_progress_state.csv')
    try:
        # Create data for the DataFrame
        data = {'letter': list(processed_letters)}
        if len(processed_letters) == 0:
            data['letter'] = [None]
        
        data['current_letter'] = [current_letter] + [None] * (len(processed_letters) - 1 if len(processed_letters) > 0 else 0)
            
        df = pd.DataFrame(data)
        df.to_csv(progress_file, index=False)
        log(f"Saved progress - processed: {sorted(processed_letters)}, current: {current_letter}")
    except Exception as e:
        log(f"Error saving progress: {e}")

# Process tables for a specific letter
def process_letter_tables(page, letter, letter_tables, output_dir, processed_tables, restart_from_table=None, batch_size=5):
    log(f"\n{'='*70}")
    log(f"PROCESSING LETTER '{letter}' - {len(letter_tables)} tables")
    log(f"{'='*70}")
    
    total_success = 0
    total_error = 0
    letter_data = []
    
    # Filter tables to only process those not already processed
    tables_to_process = []
    found_restart_point = restart_from_table is None  # If no restart point specified, start from beginning
    
    for table_name, url in letter_tables:
        # If we have a restart point and haven't found it yet
        if restart_from_table and not found_restart_point:
            if table_name == restart_from_table:
                # Found our restart point
                found_restart_point = True
            else:
                # Skip tables until we find restart point
                continue
        
        # Skip already processed tables (for safety/duplicates)
        if table_name in processed_tables:
            log(f"Skipping already processed table: {table_name}")
            continue
            
        tables_to_process.append((table_name, url))
    
    if not tables_to_process:
        log(f"No tables to process for letter '{letter}'. All tables already processed or restart point not found.")
        return 0, 0
    
    log(f"Processing {len(tables_to_process)} remaining tables for letter '{letter}'")
    
    # Process in batches
    total_batches = (len(tables_to_process) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(tables_to_process))
        batch = tables_to_process[start_idx:end_idx]
        
        log(f"\n--- LETTER {letter} - BATCH {batch_num+1}/{total_batches} ---")
        
        for i, (table_name, url) in enumerate(batch):
            try:
                table_start_time = datetime.now()
                log(f"[{i+1}/{len(batch)}] Processing: {table_name}")
                
                pk_data = extract_primary_key_data(page, table_name, url)
                
                if pk_data:
                    letter_data.extend(pk_data)
                    total_success += 1
                    log(f"  ✓ Successfully extracted {len(pk_data)} primary key columns")
                else:
                    total_error += 1
                    log(f"  ! No primary key data extracted")
                
                # Update summary file
                with open(os.path.join(output_dir, 'pk_processing_summary.csv'), 'a', newline='') as f:
                    writer = csv.writer(f)
                    processing_time = (datetime.now() - table_start_time).total_seconds()
                    writer.writerow([
                        letter, 
                        table_name, 
                        len(pk_data) if pk_data else 0, 
                        "Success" if pk_data else "No Data", 
                        "", 
                        processing_time
                    ])
                
            except Exception as e:
                total_error += 1
                log(f"  ✗ Error processing table {table_name}: {e}")
                
                # Update summary file
                with open(os.path.join(output_dir, 'pk_processing_summary.csv'), 'a', newline='') as f:
                    writer = csv.writer(f)
                    processing_time = (datetime.now() - table_start_time).total_seconds() if 'table_start_time' in locals() else 0
                    writer.writerow([letter, table_name, 0, "Error", str(e), processing_time])
            
            # Small delay between tables
            time.sleep(1)
            
            # Progress update
            total_processed = batch_num * batch_size + i + 1
            progress_pct = (total_processed / len(tables_to_process)) * 100 if tables_to_process else 0
            log(f"  Letter {letter} Progress: {progress_pct:.1f}% ({total_processed}/{len(tables_to_process)})")
        
        # Pause between batches
        if batch_num < total_batches - 1:
            log(f"Completed batch {batch_num+1}/{total_batches}. Taking a short break...")
            time.sleep(3)
    
    # Append the data for this letter to existing file or create new file
    if letter_data:
        letter_file = os.path.join(output_dir, f"pk_{letter}.csv")
        
        if os.path.exists(letter_file):
            # Append to existing file
            existing_df = pd.read_csv(letter_file)
            new_df = pd.DataFrame(letter_data)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            # Remove duplicates based on table_name and column_name
            combined_df = combined_df.drop_duplicates(subset=['table_name', 'column_name'])
            combined_df.to_csv(letter_file, index=False)
            log(f"Updated {letter_file} with {len(letter_data)} new primary key columns, total: {len(combined_df)}")
        else:
            # Create new file
            df = pd.DataFrame(letter_data)
            df.to_csv(letter_file, index=False)
            log(f"Saved {len(letter_data)} primary key columns to {letter_file}")
    else:
        log(f"No new primary key data extracted for letter '{letter}'.")
    
    log(f"\nLetter '{letter}' processing complete: {total_success} successful, {total_error} failed")
    return total_success, total_error

# Main function
def main():
    print("\n" + "="*70)
    print("EPIC EHI TABLES PRIMARY KEY EXTRACTION - RESUMING")
    print("Continuing from where the previous script left off")
    print("="*70 + "\n")
    
    # Create output directory
    output_dir = 'epic_data_primary_keys'
    os.makedirs(output_dir, exist_ok=True)
    
    # Define alphabet
    alphabet = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ') + ['SPECIAL']
    
    # Load progress
    processed_letters, current_letter = load_progress(output_dir)
    
    # Get already processed tables
    processed_tables = get_processed_tables(output_dir)
    
    # Define restart point - this is the table name we want to continue from
    restart_table = "COD_OTHER_PROV_SOURCE"
    
    with sync_playwright() as p:
        try:
            log("Launching browser...")
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()
            
            # Navigate to the index page
            index_url = "https://open.epic.com/EHITables/GetTable/_index.htm"
            log(f"Loading index page: {index_url}")
            page.goto(index_url, timeout=60000, wait_until='networkidle')
            page.wait_for_selector('a')
            
            # Find all table links
            log("Searching for table links...")
            table_links = page.query_selector_all('a[href*=".htm"]')
            log(f"Found {len(table_links)} potential links")
            
            # Extract table names and URLs
            all_tables = []
            base_url = "https://open.epic.com/EHITables/GetTable/"
            
            for link in table_links:
                href = link.get_attribute('href')
                text = link.inner_text().strip()
                
                # Skip empty or short text links
                if not text or len(text) < 2:
                    continue
                    
                # Skip index and non-table links
                if text.startswith('_') or text.lower() in ['home', 'back', 'next', 'previous']:
                    continue
                    
                # Handle both absolute and relative URLs
                url = ""
                if href and 'GetTable/_' not in href and 'GetTable/' in href:
                    if href.startswith('./'):
                        url = base_url + href[2:]
                    elif href.startswith('/'):
                        url = 'https://open.epic.com' + href
                    else:
                        url = href
                else:
                    # Construct a URL based on the text
                    url = base_url + text + '.htm'
                
                all_tables.append((text, url))
            
            # Remove duplicates
            unique_tables = []
            seen = set()
            for name, url in all_tables:
                if name not in seen:
                    seen.add(name)
                    unique_tables.append((name, url))
            
            all_tables = unique_tables
            log(f"Found {len(all_tables)} unique tables")
            
            # Group tables by first letter
            letter_groups = {letter: [] for letter in alphabet}
            for name, url in all_tables:
                if not name:
                    continue
                    
                first_letter = name[0].upper()
                if first_letter.isalpha():
                    if first_letter in letter_groups:
                        letter_groups[first_letter].append((name, url))
                else:
                    letter_groups['SPECIAL'].append((name, url))
            
            # Log table counts by letter
            log("\nTable counts by letter:")
            for letter in alphabet:
                tables = letter_groups.get(letter, [])
                if tables:  # Only show letters with tables
                    status = "DONE" if letter in processed_letters else "PENDING"
                    if letter == current_letter:
                        status = "IN PROGRESS"
                    log(f"  {letter}: {len(tables)} tables - {status}")
            
            # Make sure summary file header exists
            summary_file = os.path.join(output_dir, 'pk_processing_summary.csv')
            if not os.path.exists(summary_file):
                with open(summary_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Letter', 'Table_Name', 'PK_Column_Count', 'Status', 'Error', 'Processing_Time'])
            
            # Determine start letter
            start_index = 0
            if current_letter and current_letter in alphabet:
                # Continue from current letter with restart point
                start_index = alphabet.index(current_letter)
                log(f"Resuming from letter '{current_letter}' at table '{restart_table}'")
            else:
                # Shouldn't happen but handle just in case
                for i, letter in enumerate(alphabet):
                    if letter in processed_letters:
                        continue
                    else:
                        start_index = i
                        break
                log(f"Starting with letter '{alphabet[start_index]}'")
            
            # Process each letter in sequence
            for i in range(start_index, len(alphabet)):
                letter = alphabet[i]
                
                # Set restart table for first letter, but null for subsequent letters
                current_restart_table = restart_table if letter == current_letter else None
                
                # Skip if already processed
                if letter in processed_letters and letter != current_letter:
                    log(f"Skipping letter '{letter}' - already processed")
                    continue
                
                # Mark the current letter we're processing
                save_progress(output_dir, processed_letters, letter)
                
                # Get tables for this letter
                letter_tables = letter_groups.get(letter, [])
                
                if not letter_tables:
                    log(f"No tables found for letter '{letter}'. Marking as processed.")
                    processed_letters.add(letter)
                    save_progress(output_dir, processed_letters)
                    continue
                
                # Process tables for this letter, starting from restart table if specified
                process_letter_tables(page, letter, letter_tables, output_dir, processed_tables, current_restart_table)
                
                # Mark letter as processed
                processed_letters.add(letter)
                save_progress(output_dir, processed_letters)
                
                # Take a break between letters
                if i < len(alphabet) - 1:
                    next_letter = alphabet[i+1]
                    if next_letter not in processed_letters:
                        log(f"Taking a break before starting letter '{next_letter}'...")
                        time.sleep(10)
            
            # Final summary
            log("\n" + "="*70)
            log("PROCESSING COMPLETE")
            log(f"Processed letters: {sorted(processed_letters)}")
            letter_files = [f for f in os.listdir(output_dir) if f.endswith('.csv') and f != 'pk_processing_summary.csv']
            log(f"Letter files created: {len(letter_files)}")
            log(f"Results saved to {output_dir}/ folder")
            log("="*70)
            
        except Exception as e:
            log(f"Global error: {e}")
        finally:
            log("Closing browser...")
            browser.close()

if __name__ == "__main__":
    main()
