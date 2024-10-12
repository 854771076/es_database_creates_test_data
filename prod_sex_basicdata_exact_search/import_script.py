# -*- coding: utf-8 -*-
import requests
import json,os
# 获取当前脚本的绝对路径
current_script_path = os.path.abspath(__file__)
# 获取当前脚本的目录
current_script_directory = os.path.dirname(current_script_path)
ES_HOST = "http://10.255.87.151:9200"  # Elasticsearch 服务器地址
INDEX_NAME = "prod_sex_basicdata_exact_search"  # 索引名称

def load_data(file_path):
    # 从文件加载数据，假设数据格式是 JSON
    with open(os.path.join(current_script_directory,file_path), 'r', encoding='utf-8') as f:
        return json.load(f)
def refresh():
    url=f'http://10.255.87.151:9200/prod_sex_basicdata_exact_search/_refresh'
    headers = {'Content-Type': 'application/x-ndjson'}
    response = requests.get(url, headers=headers)
    print("索引刷新成功")
def bulk_import(data):
    url = f"http://10.255.87.151:9200/_bulk"
    headers = {'Content-Type': 'application/x-ndjson'}
    bulk_data = ""

    for doc in data:
        # 为每个文档创建 bulk 请求数据
        bulk_data += '{"index": {"_index": "prod_sex_basicdata_exact_search"}}\n'
        bulk_data += json.dumps(doc) + "\n"

    # 发送 bulk 请求导入数据
    response = requests.post(url, headers=headers, data=bulk_data)
    if response.json().get('errors',False)==False:
        print("数据导入成功")
        refresh()
    else:
        print(f"数据导入失败: {response.status_code}")
        print(response.text)

if __name__ == '__main__':
    file_path = 'data.json'
    data = load_data(file_path)
    bulk_import(data)
