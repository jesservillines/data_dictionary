import os
import time
import csv
import pandas as pd
from playwright.sync_api import sync_playwright
import re
from datetime import datetime
import sys

# Define the columns for our CSV
columns = ['table_name', 'column_name', 'primary_key', 'ordinal_position', 'type', 'discontinued', 'description']

# Function to print with timestamp
def log(message):
    """Print a message with timestamp and immediately flush output"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()

# Function to extract data from a table page
def extract_table_data(page, table_name, url):
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
                    if len(cells) >= 1:
                        column_name = cells[0].inner_text().strip()
                        primary_keys.append(column_name)
                        
                log(f"Primary keys: {primary_keys}")
                break
        
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
        
        if not column_table:
            log("Could not find any suitable column information table. Skipping table.")
            return []
        
        # Process column information
        table_data = []
        column_rows = column_table.query_selector_all('tr:not(:first-child)')
        log(f"Found {len(column_rows)} column rows")
        
        descriptions = {}
        current_column = None
        
        for i, row in enumerate(column_rows):
            cells = row.query_selector_all('td')
            
            # If first cell is a number, it's a column definition row
            if len(cells) >= 4 and cells[0].inner_text().strip().isdigit():
                position = cells[0].inner_text().strip()
                column_name = cells[1].inner_text().strip()
                data_type = cells[2].inner_text().strip()
                discontinued = cells[3].inner_text().strip()
                
                # Get description from the row
                description = ""
                if len(cells) > 4:
                    description = cells[4].inner_text().strip()
                
                current_column = column_name
                descriptions[current_column] = description
                
                # Check if this column is a primary key
                is_primary_key = 'Y' if column_name in primary_keys else 'N'
                
                # Add the row to our data
                table_data.append({
                    'table_name': table_name,
                    'column_name': column_name,
                    'primary_key': is_primary_key,
                    'ordinal_position': position,
                    'type': data_type,
                    'discontinued': discontinued,
                    'description': description
                })
            
            # If first cell is not a number, it might be a continuation of the description
            elif current_column and len(cells) > 0:
                extra_text = row.inner_text().strip()
                if extra_text:
                    # Add to existing description
                    if descriptions[current_column]:
                        descriptions[current_column] += " " + extra_text
                    else:
                        descriptions[current_column] = extra_text
        
        # Update descriptions in the table data
        for row in table_data:
            row['description'] = descriptions.get(row['column_name'], '')
        
        log(f"Successfully extracted {len(table_data)} columns for table {table_name}")
        return table_data
        
    except Exception as e:
        log(f"Error processing table {table_name}: {e}")
        return []

# Function to load progress
def load_progress(output_dir):
    progress_file = os.path.join(output_dir, 'progress_state.csv')
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
    progress_file = os.path.join(output_dir, 'progress_state.csv')
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
def process_letter_tables(page, letter, letter_tables, output_dir, batch_size=5):
    log(f"\n{'='*70}")
    log(f"PROCESSING LETTER '{letter}' - {len(letter_tables)} tables")
    log(f"{'='*70}")
    
    total_success = 0
    total_error = 0
    letter_data = []
    
    # Process in batches
    total_batches = (len(letter_tables) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(letter_tables))
        batch = letter_tables[start_idx:end_idx]
        
        log(f"\n--- LETTER {letter} - BATCH {batch_num+1}/{total_batches} ---")
        
        for i, (table_name, url) in enumerate(batch):
            try:
                table_start_time = datetime.now()
                log(f"[{i+1}/{len(batch)}] Processing: {table_name}")
                
                table_data = extract_table_data(page, table_name, url)
                
                if table_data:
                    letter_data.extend(table_data)
                    total_success += 1
                    log(f"  ✓ Successfully extracted {len(table_data)} columns")
                else:
                    total_error += 1
                    log(f"  ! No data extracted")
                
                # Update summary file
                with open(os.path.join(output_dir, 'processing_summary.csv'), 'a', newline='') as f:
                    writer = csv.writer(f)
                    processing_time = (datetime.now() - table_start_time).total_seconds()
                    writer.writerow([
                        letter, 
                        table_name, 
                        len(table_data) if table_data else 0, 
                        "Success" if table_data else "No Data", 
                        "", 
                        processing_time
                    ])
                
            except Exception as e:
                total_error += 1
                log(f"  ✗ Error processing table {table_name}: {e}")
                
                # Update summary file
                with open(os.path.join(output_dir, 'processing_summary.csv'), 'a', newline='') as f:
                    writer = csv.writer(f)
                    processing_time = (datetime.now() - table_start_time).total_seconds() if 'table_start_time' in locals() else 0
                    writer.writerow([letter, table_name, 0, "Error", str(e), processing_time])
            
            # Small delay between tables
            time.sleep(1)
            
            # Progress update
            total_processed = batch_num * batch_size + i + 1
            progress_pct = (total_processed / len(letter_tables)) * 100 if letter_tables else 0
            log(f"  Letter {letter} Progress: {progress_pct:.1f}% ({total_processed}/{len(letter_tables)})")
        
        # Pause between batches
        if batch_num < total_batches - 1:
            log(f"Completed batch {batch_num+1}/{total_batches}. Taking a short break...")
            time.sleep(3)
    
    # Save the data for this letter
    if letter_data:
        letter_file = os.path.join(output_dir, f"{letter}.csv")
        df = pd.DataFrame(letter_data)
        df.to_csv(letter_file, index=False)
        log(f"Saved {len(letter_data)} columns to {letter_file}")
    else:
        log(f"No data extracted for letter '{letter}'. CSV file not created.")
    
    log(f"\nLetter '{letter}' processing complete: {total_success} successful, {total_error} failed")
    return total_success, total_error

# Main function
def main():
    print("\n" + "="*70)
    print("EPIC EHI TABLES DATA EXTRACTION - ALPHABETICAL")
    print("Processing all alphabet letters automatically")
    print("="*70 + "\n")
    
    # Create output directory
    output_dir = 'epic_data_tables'
    os.makedirs(output_dir, exist_ok=True)
    
    # Define alphabet
    alphabet = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ') + ['SPECIAL']
    
    # Load progress
    processed_letters, current_letter = load_progress(output_dir)
    
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
                    log(f"  {letter}: {len(tables)} tables - {status}")
            
            # Create/update summary file header if doesn't exist
            summary_file = os.path.join(output_dir, 'processing_summary.csv')
            if not os.path.exists(summary_file):
                with open(summary_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Letter', 'Table_Name', 'Column_Count', 'Status', 'Error', 'Processing_Time'])
            
            # Determine start letter
            start_index = 0
            if current_letter and current_letter in alphabet:
                # Continue from current letter
                start_index = alphabet.index(current_letter)
                log(f"Continuing from letter '{current_letter}'")
            else:
                # Skip already processed letters
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
                
                # Skip if already processed
                if letter in processed_letters:
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
                
                # Process tables for this letter
                process_letter_tables(page, letter, letter_tables, output_dir)
                
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
            letter_files = [f for f in os.listdir(output_dir) if f.endswith('.csv') and f != 'processing_summary.csv']
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
