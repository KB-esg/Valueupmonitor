import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

def setup_driver():
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--headless')
    options.add_argument('--window-size=1920,1080')
    
    # User agent setup
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")
    
    try:
        if os.path.exists('/usr/bin/chromium-browser'):
            options.binary_location = '/usr/bin/chromium-browser'
            service = Service('/usr/bin/chromedriver')
        elif os.path.exists('/usr/bin/google-chrome-stable'):
            options.binary_location = '/usr/bin/google-chrome-stable'
            service = Service('/usr/bin/chromedriver')
        else:
            service = Service('/usr/bin/chromedriver')
    except Exception:
        service = Service('/usr/bin/chromedriver')
    
    return webdriver.Chrome(service=service, options=options)

def take_screenshot(url, filename):
    driver = setup_driver()
    try:
        driver.get(url)
        time.sleep(5)  # Wait for page to load
        driver.save_screenshot(filename)
        print(f"Screenshot saved as {filename}")
    except Exception as e:
        print(f"Error taking screenshot: {str(e)}")
    finally:
        driver.quit()

if __name__ == "__main__":
    # Example usage
    take_screenshot("https://www.msit.go.kr", "msit_homepage.png")
    # Add more screenshots as needed
