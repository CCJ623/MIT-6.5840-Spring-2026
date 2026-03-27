import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor

# 配置
TARGET_URL = "https://pdos.csail.mit.edu/6.824/schedule.html"
SAVE_DIR = "MIT_6824_Papers"
MAX_WORKERS = 5  # 并发下载线程数

def download_one_file(url):
    """下载单个文件的函数，供线程池调用"""
    try:
        file_name = url.split('/')[-1]
        file_path = os.path.join(SAVE_DIR, file_name)
        
        if os.path.exists(file_path):
            return f"[已跳过] {file_name}"

        response = requests.get(url, timeout=20, stream=True)
        response.raise_for_status()
        
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return f"[成功] 下载完成: {file_name}"
    except Exception as e:
        return f"[失败] {url}: {str(e)}"

def main():
    # 1. 创建保存目录
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)

    print(f"正在抓取页面: {TARGET_URL} ...")
    
    # 2. 获取网页内容
    try:
        resp = requests.get(TARGET_URL)
        resp.raise_for_status()
        # 强制指定编码，防止中文或特殊字符乱码
        resp.encoding = resp.apparent_encoding
        soup = BeautifulSoup(resp.text, 'html.parser')
    except Exception as e:
        print(f"无法访问网页: {e}")
        return

    # 3. 解析 PDF 链接
    paper_urls = set()
    # 寻找所有 class 为 reading 的 span
    reading_sections = soup.find_all('span', class_='reading')
    
    for section in reading_sections:
        # 确认该区域是否包含 "Preparation"
        text_content = section.get_text()
        if "Preparation:" in text_content:
            links = section.find_all('a')
            for link in links:
                href = link.get('href')
                if href and href.lower().endswith('.pdf'):
                    # 转换为绝对 URL
                    full_url = urljoin(TARGET_URL, href)
                    paper_urls.add(full_url)

    print(f"找到 {len(paper_urls)} 篇 Preparation 论文。开始并发下载...")

    # 4. 使用线程池并发下载
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(download_one_file, paper_urls))

    # 5. 打印结果摘要
    for res in results:
        print(res)

    print("\n" + "="*30)
    print(f"所有任务已处理。文件保存在: {os.path.abspath(SAVE_DIR)}")

if __name__ == "__main__":
    main()