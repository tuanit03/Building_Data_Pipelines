import logging
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
from pymongo import MongoClient
import time
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import random


# Kết nối MongoDB
client = MongoClient('mongodb://localhost:27017/')
# client = MongoClient('mongodb://admin:admin@localhost:27017/')

db = client['mydatabase']
collection = db['articles']

# Hàm chuyển đổi ngày tháng sang Unix timestamp
def to_unix_timestamp(date_string):
    return int(time.mktime(time.strptime(date_string, '%Y-%m-%d')))

# Hàm để lấy các liên kết bài báo từ trang mục với khoảng nghỉ giữa mỗi request
def get_article_links(cateid, from_date, to_date, page):
    try:
        logger = logging.getLogger('airflow.task')  # Sử dụng logger mặc định của Airflow
        category_url = f'https://vnexpress.net/category/day/cateid/{cateid}/fromdate/{from_date}/todate/{to_date}/allcate/{cateid}/page/{page}'

        start_time = time.time()
        # Thêm khoảng nghỉ để tránh bị chặn
        time.sleep(random.uniform(0, 16))

        response = requests.get(category_url, timeout = 16)
        response_time = time.time() - start_time
        logger.info(f'Request page {page} completed in {response_time:.2f} seconds')

        soup = BeautifulSoup(response.content, 'html.parser')

        article_links = []
        titles = soup.find_all('h3', class_='title-news')
        for title in titles:
            a = title.find('a')
            link = a['href']
            if link:
                article_links.append(link)

        return article_links
    except Exception as e:
        logger.error(f'Lỗi khi lấy liên kết bài báo trang {page}: {e}')
        return []

# Hàm cào nội dung bài báo với khoảng nghỉ giữa các request
@retry(stop=stop_after_attempt(2), wait=wait_fixed(0), retry=retry_if_exception_type(requests.exceptions.RequestException))
def scrape_article(link, category_name):
    logger = logging.getLogger('airflow.task')  # Sử dụng logger mặc định của Airflow
    data = {}
    try:
        start_time = time.time()
        # Thêm khoảng nghỉ để tránh bị chặn
        time.sleep(random.uniform(0, 16))
        response = requests.get(link, timeout = 16)
        response_time = time.time() - start_time
        logger.info(f'Request article {link} completed in {response_time:.2f} seconds')

        soup = BeautifulSoup(response.content, 'html.parser')

        data['category'] = category_name
        paragraphs = soup.select('article.fck_detail p.Normal')
        data['content'] = " ".join([p.get_text().strip() for p in paragraphs])

        date_span = soup.select_one('div.header-content.width_common span.date')
        data['date_time'] = date_span.get_text().strip() if date_span else None

        title_h1 = soup.select_one('h1.title-detail')
        data['title'] = title_h1.get_text().strip() if title_h1 else None


    except Exception as e:
        logger.error(f"Lỗi khi cào nội dung từ {link}: {e}")
        return None

    return data

# Hàm transform dữ liệu
def transform_article(article):
    logger = logging.getLogger('airflow.task')  # Sử dụng logger mặc định của Airflow
    start_time = time.time()
    content_transformed = re.sub(r'<[^>]*>', '', article['content'])
    content_transformed = re.sub(r'\s+', ' ', content_transformed)
    article['content'] = ' '.join(content_transformed.split())
    transform_time = time.time() - start_time
    logger.info(f'Transform completed in {transform_time:.2f} seconds')
    return article

# Hàm để thực hiện toàn bộ quy trình ETL cho một cặp category, năm và tháng
def etl_category(cateid, category_name, year, month, **context):
    logger = logging.getLogger('airflow.task')  # Sử dụng logger mặc định của Airflow

    logger.info(f'Starting ETL for category: {category_name}, year: {year}, month: {month}')
    from_date = to_unix_timestamp(f'{year}-{month:02d}-01')
    to_date = to_unix_timestamp(f'{year}-{month:02d}-28')

    for page in range(1, 21):  # Duyệt qua 20 trang
        logger.info(f'Starting to fetch page {page}')
        article_links = get_article_links(cateid, from_date, to_date, page)

        if not article_links:
            logger.info(f"Không có bài viết nào trong {category_name} tháng {month} năm {year} trên trang {page}")
            break

        # Cào nội dung và lưu trực tiếp vào MongoDB
        for link in article_links:
            article = scrape_article(link, category_name)
            if article:
                # Transform dữ liệu
                transformed_article = transform_article(article)

                start_time = time.time()
                collection.insert_one(transformed_article)
                db_time = time.time() - start_time
                logger.info(f'Inserted article into MongoDB in {db_time:.2f} seconds')
            else:
                logger.warning(f"Bỏ qua bài viết từ link: {link}")

    logger.info(f'ETL completed for category: {category_name}, year: {year}, month: {month}')

# Định nghĩa DAG trong Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Định nghĩa DAG
with DAG(
        'category_etl_pipeline',
        default_args=default_args,
        description='Extract, Transform, Load articles from vnexpress by category, year, and month',
        schedule_interval=None,  # Chạy khi được trigger thủ công
        catchup=False
) as dag:
    # Định nghĩa các category
    categories = [
        {'cateid': '1003159', 'name': 'kinh_doanh'},
        {'cateid': '1001005', 'name': 'thoi_su'},
        {'cateid': '1001002', 'name': 'the_gioi'},
        {'cateid': '1005628', 'name': 'bat_dong_san'},
        {'cateid': '1002691', 'name': 'giai_tri'},
        {'cateid': '1003497', 'name': 'giao_duc'},
        {'cateid': '1001009', 'name': 'khoa_hoc'},
        {'cateid': '1002565', 'name': 'the_thao'},
        {'cateid': '1001007', 'name': 'phap_luat'},
        {'cateid': '1003750', 'name': 'suc_khoe'},
        {'cateid': '1002966', 'name': 'doi_song'},
        {'cateid': '1003231', 'name': 'du_lich'}
    ]

    # Tạo các task ETL độc lập cho từng cặp category, year, month
    for category in categories:
        for year in range(2020, 2021):  # Các năm từ 2020 đến 2023
            for month in range(1, 13):  # Các tháng từ 1 đến 12
                task_id = f"etl_{category['name']}_{year}_{month}"

                etl_task = PythonOperator(
                    task_id=task_id,
                    python_callable=etl_category,
                    op_kwargs={
                        'cateid': category['cateid'],
                        'category_name': category['name'],
                        'year': year,
                        'month': month
                    },
                    dag=dag
                )
