import pandas as pd
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager

print("Initializing WebDriver...")
DRIVER_PATH = ChromeDriverManager().install()

# -------------------------------------------------
# Browser creator (fast + headless)
# -------------------------------------------------

def get_driver():
    options = Options()

    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    options.add_argument("--blink-settings=imagesEnabled=false")

    options.page_load_strategy = "eager"

    return webdriver.Chrome(service=Service(DRIVER_PATH), options=options)


# -------------------------------------------------
# Cleaning helpers
# -------------------------------------------------

def clean_rating(text):
    if not text:
        return ""
    m = re.search(r"([\d.]+)", text)
    return m.group(1) if m else ""


def clean_reviews(text):
    if not text:
        return ""
    return re.sub(r"[^\d]", "", text)


def clean_date(text):
    if not text:
        return ""
    try:
        d = datetime.strptime(text.strip(), "%B %d, %Y")
        return d.strftime("%Y-%m-%d")
    except:
        return text.strip()


# -------------------------------------------------
# Scrape book detail pages
# -------------------------------------------------

def scrape_book_details(book):

    if not book["URL"]:
        return book

    driver = get_driver()

    try:

        driver.get(book["URL"])

        soup = BeautifulSoup(driver.page_source, "lxml")

        # description
        desc = soup.select_one("#bookDescription_feature_div span")

        if desc:
            book["Description"] = desc.get_text(strip=True)

        # publisher + date
        details = soup.select("#detailBulletsWrapper_feature_div li")

        for d in details:

            text = d.get_text(strip=True)

            if "Publisher" in text:

                pub = text.split("Publisher")[-1].replace(":", "").strip()
                book["Publisher"] = pub.split("(")[0].strip()

            if "Publication date" in text:

                date = text.split("Publication date")[-1].replace(":", "").strip()
                book["Publication Date"] = clean_date(date)

    except Exception as e:
        print("Failed:", book["Title"][:40], "|", e)

    finally:
        driver.quit()

    return book


# ==========================================================
# MAIN SCRIPT
# ==========================================================

if __name__ == "__main__":

    urls = [
        "https://www.amazon.com/Best-Sellers-Kindle-Store-Paranormal-Romance/zgbs/digital-text/6190484011",
        "https://www.amazon.com/Best-Sellers-Kindle-Store-Paranormal-Romance/zgbs/digital-text/6190484011/ref=zg_bs_pg_2_digital-text?_encoding=UTF8&pg=2"
    ]

    books_data = []

    print("STEP 1 — Scraping Bestseller Pages")

    driver = get_driver()

    for page_num, url in enumerate(urls, 1):

        print(f"Loading Page {page_num}")

        driver.get(url)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".zg-grid-general-faceout"))
        )

        # scroll for lazy loading
        for i in range(1, 6):

            driver.execute_script(
                f"window.scrollTo(0, document.body.scrollHeight * ({i}/5));"
            )

            time.sleep(0.5)

        soup = BeautifulSoup(driver.page_source, "lxml")

        books = soup.select(".zg-grid-general-faceout")

        for book in books:

            link = book.select_one("a")

            raw_url = "https://www.amazon.com" + link["href"] if link else ""

            clean_url = raw_url.split("?")[0].split("ref=")[0] if raw_url else ""

            rank_elem = book.select_one("span.zg-badge-text")

            rating_elem = book.select_one(".a-icon-alt")

            review_elem = book.select_one("a.a-size-small.a-link-normal")

            author_elem = book.select_one(".a-row.a-size-small")

            books_data.append({

                "Rank": rank_elem.text.replace("#", "").strip() if rank_elem else "",

                "Title": book.select_one("img")["alt"] if book.select_one("img") else "",

                "Author": author_elem.text.strip() if author_elem else "",

                "Rating": clean_rating(rating_elem.text if rating_elem else ""),

                "Reviews": clean_reviews(review_elem.text if review_elem else ""),

                "Price": book.select_one(".p13n-sc-price").text if book.select_one(".p13n-sc-price") else "",

                "URL": clean_url,

                "Description": "",
                "Publisher": "",
                "Publication Date": ""
            })

    driver.quit()

    print(f"Collected {len(books_data)} books")

    print("STEP 2 — Scraping Individual Book Pages (Parallel)")

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(scrape_book_details, books_data))

# Create dataframe
    df = pd.DataFrame(results)

# ------------------------------
# DATA CLEANING (Assignment Part 3)
# ------------------------------

# Convert rating to numeric
    df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")

# Convert reviews to numeric
    df["Reviews"] = pd.to_numeric(df["Reviews"], errors="coerce")

# Ensure URLs are clean
    df["URL"] = df["URL"].str.strip()

# Handle missing values gracefully
    df.fillna("", inplace=True)

# ------------------------------
# SAVE FINAL DATASET
# ------------------------------

    df.to_csv("romantasy_dataset_final.csv", index=False)

    print("SUCCESS — Dataset saved to romantasy_dataset_final.csv")