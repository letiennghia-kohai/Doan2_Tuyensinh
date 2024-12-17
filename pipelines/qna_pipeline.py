import os
import json
import re
from pymongo import MongoClient
from etls.qna_etl import extract_qna

# Configuration
BASE_DATA_PATH = "/opt/airflow/data/qna_data/"
MONGO_URI = "mongodb+srv://mongodb:mongodb@cluster0.eiuib.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "chatbot_db"
COLLECTION_NAME = "faq_data"
# Base URL and number of pages for FAQs
BASE_URL = 'https://ts.hust.edu.vn/p/hoi-dap?page='
TOTAL_PAGES = 141  # Số lượng trang bạn muốn lấy dữ liệu

def clean_text(text):
    """Clean text by removing HTML tags, scripts, and unnecessary whitespaces."""
    if not text:
        return ""
    # Remove HTML tags
    text = re.sub(r"<.*?>", "", text)
    # Remove JavaScript or script tags
    text = re.sub(r"<script.*?>.*?</script>", "", text, flags=re.DOTALL)
    # Remove unnecessary whitespaces
    text = re.sub(r"\s+", " ", text).strip()
    return text

def process_qna_data(stage, **context):
    """
    Process Q&A data in two stages:
    Stage 1: Extract and initial preprocessing
    Stage 2: Transform and load to MongoDB
    """
    if stage == 1:
        return process_qna_stage1()
    elif stage == 2:
        return process_qna_stage2()
    else:
        raise ValueError(f"Invalid stage: {stage}")

def process_qna_stage1():
    """
    Stage 1: Extract raw FAQ data and perform initial cleaning
    - Read the original JSON file
    - Perform basic data validation
    - Prepare data for further processing
    """
    extract_qna(BASE_URL,TOTAL_PAGES)
    input_file = os.path.join(BASE_DATA_PATH, "faqs_with_subanswers.json")
    
    # Ensure data directory exists
    os.makedirs(BASE_DATA_PATH, exist_ok=True)
    
    try:
        # Read original data
        with open(input_file, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # Basic data validation
        if not raw_data:
            print("No data found in the input file.")
            return None
        
        # Basic initial cleaning
        cleaned_data = []
        for item in raw_data:
            cleaned_item = {
                "question_title": clean_text(item.get("question_title", "")),
                "detailed_question": clean_text(item.get("detailed_question", "")),
                "raw_sub_answers": item.get("sub_answers", [])
            }
            cleaned_data.append(cleaned_item)
        
        # Save preprocessed data
        preprocessed_file = os.path.join(BASE_DATA_PATH, "preprocessed_faqs.json")
        with open(preprocessed_file, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, ensure_ascii=False, indent=4)
        
        print(f"Preprocessed {len(cleaned_data)} FAQ items")
        return preprocessed_file
    
    except Exception as e:
        print(f"Error in Q&A Stage 1 processing: {e}")
        return None

def process_qna_stage2():
    """
    Stage 2: Transform and load processed FAQ data to MongoDB
    - Read preprocessed data
    - Deduplicate and structure answers
    - Load to MongoDB
    """
    preprocessed_file = os.path.join(BASE_DATA_PATH, "preprocessed_faqs.json")
    
    try:
        # Read preprocessed data
        with open(preprocessed_file, 'r', encoding='utf-8') as f:
            preprocessed_data = json.load(f)
        
        # Transform and deduplicate data
        transformed_data = []
        for item in preprocessed_data:
            # Deduplicate answers
            seen_answers = set()
            unique_answers = []
            
            for ans in item.get("raw_sub_answers", []):
                content = clean_text(ans.get("sub_answer", ""))
                if content and content not in seen_answers:
                    seen_answers.add(content)
                    unique_answers.append({
                        "author": clean_text(ans.get("author", "Unknown")),
                        "sub_answer": content
                    })
            
            transformed_item = {
                "question_title": item.get("question_title", ""),
                "detailed_question": item.get("detailed_question", ""),
                "answers": unique_answers
            }
            
            transformed_data.append(transformed_item)
        
        # Load to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        
        # Clear existing data and insert new data
        collection.delete_many({})
        if transformed_data:
            collection.insert_many(transformed_data)
            print(f"Loaded {len(transformed_data)} FAQ items to MongoDB")
        else:
            print("No data to load")
        
        return len(transformed_data)
    
    except Exception as e:
        print(f"Error in Q&A Stage 2 processing: {e}")
        return 0
