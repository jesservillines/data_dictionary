from playwright.sync_api import sync_playwright
import os

def capture_page_info():
    with sync_playwright() as p:
        # Launch a browser instance
        browser = p.chromium.launch(headless=False)  # Use headless=False to see the browser
        context = browser.new_context()
        page = context.new_page()
        
        # Navigate to a specific table page (using ABN_FOLLOW_UP as an example)
        url = "https://open.epic.com/EHITables/GetTable/ABN_FOLLOW_UP.htm"
        print(f"Navigating to {url}")
        page.goto(url, timeout=60000, wait_until='networkidle')
        
        # Take a screenshot
        os.makedirs("screenshots", exist_ok=True)
        page.screenshot(path="screenshots/page_screenshot.png")
        print("Screenshot saved to screenshots/page_screenshot.png")
        
        # Save the HTML content
        html_content = page.content()
        with open("page_content.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        print("HTML content saved to page_content.html")
        
        # Print page information
        print("\nPage Title:", page.title())
        
        # Check for frames
        frames = page.frames
        print(f"\nNumber of frames: {len(frames)}")
        
        # Find all tables
        tables = page.query_selector_all('table')
        print(f"\nNumber of tables found: {len(tables)}")
        
        # Try different selectors to see what works
        print("\nTrying different selectors:")
        selectors_to_try = [
            'h2',
            'table',
            'tr',
            'td',
            '.Column',
            '#content',
            'div'
        ]
        
        for selector in selectors_to_try:
            elements = page.query_selector_all(selector)
            print(f"  {selector}: {len(elements)} elements found")
        
        # Wait for user input before closing
        input("\nPress Enter to close the browser...")
        browser.close()

if __name__ == "__main__":
    capture_page_info()
