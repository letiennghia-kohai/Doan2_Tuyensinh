import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
# Import the crawler and preprocessor classes from your existing scripts
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests
import warnings
import re
import time
import io
import json
import pymongo
import nltk
from nltk.tokenize import sent_tokenize

# Download NLTK resources
nltk.download('punkt', quiet=True)
nltk.download('punkt_tab')

# Ignore SSL warnings
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

class NotificationCrawler:
    def __init__(self, base_url="https://ts.hust.edu.vn/p/dai-hoc", output_folder='/opt/airflow/data/notification_data/'):
        """
        Initialize the notification crawler
        
        :param base_url: Base URL to crawl notifications from
        :param output_folder: Folder to save downloaded files
        """
        self.base_url = base_url
        self.output_folder = output_folder
        
        # Create output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        
        # # Set up Chrome options
        # self.chrome_options = webdriver.ChromeOptions()
        # self.chrome_options.add_argument("--headless")
        # self.chrome_options.add_argument("--no-sandbox")
        # self.chrome_options.add_argument("--disable-dev-shm-usage")

    def sanitize_filename(self, url, count):
        """
        Create a sanitized filename for the downloaded content
        
        :param url: Source URL
        :param count: Unique counter
        :return: Sanitized filename
        """
        sanitized_name = re.sub(r'[^a-zA-Z0-9]', '_', url)
        return f"content_{count}_{sanitized_name[:50]}.txt"

    def clean_text(self, text):
        """
        Clean the extracted text
        
        :param text: Raw text
        :return: Cleaned text
        """
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'[^\w\s.,;:()/-]', '', text)
        return text

    def process_table(self, table, text_buffer):
        """
        Process HTML tables
        
        :param table: BeautifulSoup table element
        :param text_buffer: IO buffer to write processed text
        """
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['th', 'td'])
            for i, cell in enumerate(cells):
                text_buffer.write(self.clean_text(cell.get_text()))
                if i < len(cells) - 1:
                    text_buffer.write(' | ')
            text_buffer.write('\n')
        text_buffer.write('\n')

    def process_content(self, element, text_buffer):
        """
        Recursively process HTML content
        
        :param element: BeautifulSoup element
        :param text_buffer: IO buffer to write processed text
        """
        for child in element.children:
            if isinstance(child, str) and child.strip():
                text_buffer.write(self.clean_text(child) + ' ')
            elif child.name in ['span','h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                text_buffer.write(self.clean_text(child.get_text()) + ' ')
            elif child.name in ['div', 'p', 'ul', 'ol', 'li','center']:
                self.process_content(child, text_buffer)
                text_buffer.write('\n\n')
            elif child.name == 'br':
                text_buffer.write('\n')
            elif child.name == 'table':
                self.process_table(child, text_buffer)

    def get_total_pages(self, driver, url):
        """
        Determine total number of pages
        
        :param driver: Selenium WebDriver
        :param url: URL to crawl
        :return: Total number of pages
        """
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "content")))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        pagination = soup.select('ul.pagination a[href]')
        last_page = 1

        for link in pagination:
            try:
                page_num = int(link.get_text())
                last_page = max(last_page, page_num)
            except ValueError:
                continue
        
        return last_page

    # def crawl_notifications(self, chrome_driver_path='/opt/airflow/data/input/chromedriver.exe'):
    #     """
    #     Crawl notifications from multiple pages
        
    #     :param chrome_driver_path: Path to ChromeDriver executable
    #     :return: Number of crawled notifications
    #     """
    #     # Set up Chrome service
    #     chrome_service = ChromeService(executable_path=chrome_driver_path)
    #     driver = webdriver.Chrome(service=chrome_service, options=self.chrome_options)
        
    #     count = 1
    #     total_notifications_crawled = 0

    #     try:
    #         # Determine total pages
    #         # total_pages = self.get_total_pages(driver, self.base_url)
    #         total_pages = 2
    #         print(f"Total pages found: {total_pages}")

    #         # Crawl each page
    #         for page_num in range(1, total_pages + 1):
    #             page_url = f"{self.base_url}?page={page_num}"
    #             print(f"Accessing: {page_url}")
                
    #             driver.get(page_url)
    #             WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "content")))
    #             soup = BeautifulSoup(driver.page_source, 'html.parser')

    #             # Find article links
    #             links = soup.select('h3 a[href]')

    #             for link in links:
    #                 article_url = link['href']
    #                 if not article_url.startswith('http'):
    #                     article_url = "https://ts.hust.edu.vn" + article_url
                    
    #                 try:
    #                     response = requests.get(article_url, verify=False)
    #                     response.encoding = 'utf-8'
    #                     soup = BeautifulSoup(response.text, 'html.parser')
    #                     content = soup.find('div', id='content')
                        
    #                     if content:
    #                         text_buffer = io.StringIO()
    #                         self.process_content(content, text_buffer)
                            
    #                         file_name = self.sanitize_filename(article_url, count)
    #                         full_path = os.path.join(self.output_folder, file_name)
                            
    #                         with open(full_path, "w", encoding="utf-8") as file:
    #                             file.write(text_buffer.getvalue())
                            
    #                         total_notifications_crawled += 1
    #                         count += 1
                        
    #                 except Exception as e:
    #                     print(f"Error crawling {article_url}: {e}")
                    
    #                 time.sleep(1)  # Delay between crawls

    #     finally:
    #         driver.quit()
        
    #     return total_notifications_crawled

class NotificationPreprocessor:
    def __init__(self, input_folder='/opt/airflow/data/input/', output_file='/opt/airflow/data/notification_data/notifications.json'):
        """
        Initialize notification preprocessor
        
        :param input_folder: Folder with raw text files
        :param output_file: Output JSON file
        """
        self.input_folder = input_folder
        self.output_file = output_file
        self.processed_data = []

    def clean_text(self, text):
        """
        Normalize and clean text
        
        :param text: Input text
        :return: Cleaned text
        """
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'[^a-zA-ZÀ-ỹ0-9\s.,!?():]', '', text)
        return text.strip()

    def extract_date(self, text):
        """
        Extract date from text
        
        :param text: Input text
        :return: Extracted date
        """
        date_patterns = [
            r'\d{1,2}/\d{1,2}/\d{4}',  # DD/MM/YYYY
            r'\d{4}-\d{1,2}-\d{1,2}',  # YYYY-MM-DD
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    date = match.group(0)
                    for fmt in ['%d/%m/%Y', '%Y-%m-%d']:
                        try:
                            return datetime.strptime(date, fmt).strftime('%Y-%m-%d')
                        except ValueError:
                            continue
                except Exception:
                    pass
        
        return datetime.now().strftime('%Y-%m-%d')

    def categorize_notification(self, text):
        """
        Categorize notification
        
        :param text: Input text
        :return: Category
        """
        categories = {
            'Thông báo tuyển sinh': ['tuyển sinh', 'xét tuyển', 'đăng ký', 'nhập học'],
            'Hướng dẫn': ['hướng dẫn', 'quy trình', 'thủ tục', 'hỗ trợ'],
            'Thông tin chung': ['thông báo', 'tin tức', 'cập nhật']
        }
        
        text_lower = text.lower()
        for category, keywords in categories.items():
            if any(keyword in text_lower for keyword in keywords):
                return category
        
        return 'Thông tin chung'

    def process_notifications(self):
        """
        Process all notification files
        
        :return: Number of processed notifications
        """
        for filename in os.listdir(self.input_folder):
            if filename.endswith('.txt'):
                filepath = os.path.join(self.input_folder, filename)
                
                try:
                    with open(filepath, 'r', encoding='utf-8') as file:
                        raw_text = file.read()
                    
                    cleaned_text = self.clean_text(raw_text)
                    sentences = sent_tokenize(cleaned_text)
                    
                    title = ' '.join(sentences[:2]) if sentences else 'Không có tiêu đề'
                    content = ' '.join(sentences[2:]) if len(sentences) > 2 else cleaned_text
                    
                    notification = {
                        "title": title,
                        "content": content,
                        "category": self.categorize_notification(content),
                        "date_published": self.extract_date(content),
                        "source": f"file://{filepath}"
                    }
                    
                    self.processed_data.append(notification)
                
                except Exception as e:
                    print(f"Error processing {filename}: {e}")
        
        return len(self.processed_data)

    def save_to_json(self):
        """
        Save processed notifications to JSON
        """
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(self.processed_data, f, ensure_ascii=False, indent=2)
        print(f"Processed {len(self.processed_data)} notifications.")

    def save_to_mongodb(self, connection_string='mongodb+srv://mongodb:mongodb@cluster0.eiuib.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0', db_name='chatbot_db', collection_name='noti_data'):
        """
        Save processed data to MongoDB Atlas
        
        :param connection_string: MongoDB Atlas connection string
        :param db_name: Database name
        :param collection_name: Collection name
        """
        try:
            # Kết nối tới MongoDB Atlas
            client = MongoClient(connection_string)
            db = client[db_name]
            collection = db[collection_name]
            
            # Xóa dữ liệu hiện tại (nếu cần)
            collection.delete_many({})
            
            # Thêm dữ liệu mới
            if self.processed_data:
                collection.insert_many(self.processed_data)
                print(f"Saved {len(self.processed_data)} notifications to MongoDB Atlas.")
            else:
                print("No data to load into MongoDB Atlas.")
            
            # Đóng kết nối
            client.close()
        except Exception as e:
            print(f"MongoDB Atlas error: {e}")


import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def crawl_notifications_task():
    logger.info("Starting crawl notifications task.")
    try:
        crawler = NotificationCrawler()
        total_crawled = crawler.output_folder
        logger.info(f"Total notifications crawled: {total_crawled}")
    except Exception as e:
        logger.error(f"Error in crawl_notifications_task: {e}")
        raise

def process_notifications_task():
    logger.info("Starting process notifications task.")
    try:
        preprocessor = NotificationPreprocessor()
        total_processed = preprocessor.process_notifications()
        preprocessor.save_to_json()
        preprocessor.save_to_mongodb()
        logger.info(f"Total notifications processed: {total_processed}")
    except Exception as e:
        logger.error(f"Error in process_notifications_task: {e}")
        raise
