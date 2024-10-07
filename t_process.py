import logging
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import py_vncorenlp


# Kết nối MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['my_database']
collection = db['articles']  # Collection gốc
transformed_collection = db['transformed_articles']  # Collection mới cho dữ liệu đã transform

model_path = "~/building_data_pipelines/VnCoreNLP-master"
rdrsegmenter = py_vncorenlp.VnCoreNLP(save_dir=model_path, annotators=["wseg"])
top_words = ["cua","cho","trong","cac","duoc","voi","nam","nhung","mot","khong","nay","nguoi","den","cung","khi","tai","nhieu","vao","tren","hon","moi","con","noi","co_the", "ngay","sau","toi","thang","nhu","dong"]

# Hàm loại bỏ HTML tags
def remove_html(txt):
    return re.sub(r'<[^>]*>', '', txt)

# Hàm chuyển Unicode tiếng Việt sang dạng không dấu
def convert_unicode(txt):
    return unidecode(txt)

def remove_proper_nouns(txt):
    # Regex để xóa từ có chứa bất kỳ chữ cái in hoa nào
    pattern = r'\b\w*[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠƯ]\w*\b'
    text = re.sub(pattern, '', txt)
    text = re.sub(r'\b\w{1,2}\b', '', text)
    return text.strip()

def remove_special_characters(text):
    text = re.sub(r'\b(\d+\w+|\w+\d+)\b', '', text)
    text = re.sub(r'\d+', '', text)
    text = re.sub(r'\.', '', text)
    text = re.sub(r'\W+', ' ', text)
    return text.strip()

def GD1(document):
    text = remove_html(document)
    text = remove_special_characters(text)
    return text

def GD2(document):
    text = convert_unicode(document)
    text = remove_proper_nouns(text)
    return text

# Hàm xóa số và khoảng trắng thừa trong content
def clean_content(content):
    # clean_GD1
    content = GD1(content)

    output = rdrsegmenter.word_segment(content)
    result = " ".join(output)

    # clean_GD2
    content = GD2(result)

    pattern = r'\b(' + '|'.join(re.escape(word) for word in top_words) + r')\b'
    content = re.sub(pattern, '', content)
    content = re.sub(r'\s+', ' ', content).strip()

    return content

# Hàm thực hiện transform cho từng cặp category, year, month
def transform_category_month_year(category_name, year, month, **context):
    logger = logging.getLogger('airflow.task')

    # Query các bài viết theo category, year, month
    query = {
        'category': category_name,
        'year': year,
        'month': month
    }
    projection = {
        'category': 1,
        'content': 1,
        'year': 1
    }
    cursor = collection.find(query, projection)

    # Thực hiện transform dữ liệu và lưu vào collection mới
    for article in cursor:
        if article['content']:  # Bỏ qua các bài viết có content rỗng
            transformed_content = clean_content(article['content'])
            transformed_data = {
                'category': article['category'],
                'content': transformed_content,
                'year': article['year']
            }
            # Ghi từng bài viết một vào collection mới để giảm tải lên database
            transformed_collection.insert_one(transformed_data)
            logger.info(f'Inserted transformed article into new collection for category: {category_name}, year: {year}, month: {month}')

# Định nghĩa DAG transform
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
        'transform_pipeline',
        default_args=default_args,
        description='Transform articles by category, year, and month, and clean content',
        schedule_interval=None,  # Chạy khi được trigger thủ công
        catchup=False
) as dag:
    
    # Định nghĩa các category và các năm
    categories = [
        'kinh_doanh', 'thoi_su', 'the_gioi', 'bat_dong_san', 'giai_tri', 
        'giao_duc', 'khoa_hoc', 'the_thao', 'phap_luat', 'suc_khoe', 
        'doi_song', 'du_lich'
    ]
    years = range(2020, 2021)  # Các năm từ 2020 trở đi, có thể mở rộng thêm
    months = range(1, 13)  # Các tháng từ 1 đến 12

    # Tạo các task transform độc lập cho từng cặp category, year, month
    for category in categories:
        for year in years:
            for month in months:
                task_id = f'transform_{category}_{year}_{month}'

                transform_task = PythonOperator(
                    task_id=task_id,
                    python_callable=transform_category_month_year,
                    op_kwargs={
                        'category_name': category,
                        'year': year,
                        'month': month
                    },
                    dag=dag
                )
