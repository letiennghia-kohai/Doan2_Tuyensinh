import requests
from bs4 import BeautifulSoup
import json
import re
import time
import warnings

# Bỏ qua các cảnh báo SSL
warnings.filterwarnings("ignore")

def clean_text(text):
    """Làm sạch văn bản: loại bỏ các ký tự đặc biệt và thừa thãi."""
    text = re.sub(r'\s+', ' ', text)  # Thay thế nhiều khoảng trắng bằng một khoảng trắng duy nhất
    text = text.replace('\n', ' ').strip()  # Loại bỏ khoảng trắng đầu và cuối
    return text

# Function to get FAQ detail along with sub-answers
def get_faq_details(faq_url):
    try:
        response = requests.get(faq_url, verify=False)
        response.raise_for_status()  # Kiểm tra lỗi HTTP
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract question title (tiêu đề)
        question_title = soup.find('h2', class_='page-title').text.strip()

        # Extract detailed question (câu hỏi cụ thể)
        answer_div = soup.find('div', class_='wrap-content')
        detailed_question = clean_text(answer_div.get_text(separator='\n').strip()) if answer_div else "No detailed question available"

        # Extract sub-answers (câu trả lời) from multiple pages
        sub_answers = []
        current_page = 1

        while True:
            comment_section = soup.find('div', class_='comments')
            if comment_section:
                comment_items = comment_section.select('div.item-cmt')
                for item in comment_items:
                    sub_answer = clean_text(item.find('div', class_='cmt').get_text(separator='\n').strip())
                    author = clean_text(item.find('h4', class_='user').get_text(strip=True))
                    sub_answers.append({'author': author, 'sub_answer': sub_answer})

            # Check for next page link
            next_page_link = soup.find('a', {'rel': 'next'})  # Tìm link chứa rel="next"
            if next_page_link:
                next_page_url = next_page_link.get('href')
                response = requests.get(next_page_url, verify=False)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                current_page += 1
                time.sleep(1)  # Thêm thời gian chờ để không gây tải máy chủ quá nhiều
            else:
                break

        return clean_text(question_title), detailed_question, sub_answers
    except Exception as e:
        print(f"Lỗi khi truy cập {faq_url}: {e}")
        return None, None, []

# List to store the data
data = []

# Base URL and number of pages for FAQs
base_url = 'https://ts.hust.edu.vn/p/hoi-dap?page='
total_pages = 141  # Số lượng trang bạn muốn lấy dữ liệu
def extract_qna(base_url, total_pages):
    data = []
    # Iterate through pages
    for page in range(1, total_pages + 1):
        url = f"{base_url}{page}"
        response = requests.get(url, verify=False)
        response.raise_for_status()  # Kiểm tra lỗi HTTP
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract FAQ links
        links = soup.select('h4.faq-title a')

        for link in links:
            faq_url = link.get('href')
            full_url = f"{faq_url}"
            question_title, detailed_question, sub_answers = get_faq_details(full_url)
            
            if question_title and detailed_question:  # Kiểm tra nếu có dữ liệu hợp lệ
                data.append({
                    'question_title': question_title,  # Tiêu đề câu hỏi
                    'detailed_question': detailed_question,  # Câu hỏi cụ thể
                    'sub_answers': sub_answers  # Danh sách các câu trả lời (câu hỏi con)
                })

    # Lưu dữ liệu vào file JSON
    with open('/opt/airflow/data/qna_data/faqs_with_subanswers.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print("Đã lưu dữ liệu vào 'faqs_with_subanswers.json'.")


