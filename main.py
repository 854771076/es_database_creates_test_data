import requests
import json
import os
import pathlib
# 配置 Elasticsearch 服务器
ES_HOST = "http://10.255.87.151:9200"  # 请替换为你的 Elasticsearch 地址
INDEX_NAME = 'prod_sex_basicdata_exact_search'  # 输入要获取 mapping 的索引
path=pathlib.Path(INDEX_NAME)
if not path.exists():
    path.mkdir()
OUTPUT_SCRIPT = f"{INDEX_NAME}/import_script.py"  # 生成的导入脚本文件名
OUTPUT_TEMPLATE = f"{INDEX_NAME}/data.json"  # 生成的数据模板文件名

# 获取索引的 mapping
def get_mapping(index_name):
    url = f"{ES_HOST}/{index_name}/_mapping"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"获取索引 {index_name} mapping 失败: {response.status_code}")
        return None

# 根据 mapping 生成导入脚本
def generate_import_script(mapping, index_name):
    script = f'''# -*- coding: utf-8 -*-
import requests
import json,os
# 获取当前脚本的绝对路径
current_script_path = os.path.abspath(__file__)
# 获取当前脚本的目录
current_script_directory = os.path.dirname(current_script_path)
ES_HOST = "{ES_HOST}"  # Elasticsearch 服务器地址
INDEX_NAME = "{index_name}"  # 索引名称

def load_data(file_path):
    # 从文件加载数据，假设数据格式是 JSON
    with open(os.path.join(current_script_directory,file_path), 'r', encoding='utf-8') as f:
        return json.load(f)
def refresh():
    url=f'{ES_HOST}/{INDEX_NAME}/_refresh'
    headers = {{'Content-Type': 'application/x-ndjson'}}
    response = requests.get(url, headers=headers)
    print("索引刷新成功")
def bulk_import(data):
    url = f"{ES_HOST}/_bulk"
    headers = {{'Content-Type': 'application/x-ndjson'}}
    bulk_data = ""

    for doc in data:
        # 为每个文档创建 bulk 请求数据
        bulk_data += '{{"index": {{"_index": "{index_name}"}}}}\\n'
        bulk_data += json.dumps(doc) + "\\n"

    # 发送 bulk 请求导入数据
    response = requests.post(url, headers=headers, data=bulk_data)
    if response.json().get('errors',False)==False:
        print("数据导入成功")
        refresh()
    else:
        print(f"数据导入失败: {{response.status_code}}")
        print(response.text)

if __name__ == '__main__':
    file_path = 'data.json'
    data = load_data(file_path)
    bulk_import(data)
'''
    return script

# 生成数据模板
def generate_data_template(mapping, index_name):
    properties = list(mapping.values())[0]['mappings']['properties']
    template = {}

    def build_template(properties):
        template = {}
        
        for field, details in properties.items():
            flag=True
            for k in template.keys():
                if k.upper()==field.upper():
                    flag=False
            if flag:
                field_type = details.get('type')
                if field_type == 'text':
                    template[field] = "示例文本"
                elif field_type == 'keyword':
                    template[field] = "示例关键字"
                elif field_type == 'integer':
                    template[field] = 0
                elif field_type == 'long':
                    template[field] = 0
                elif field_type == 'float':
                    template[field] = 0.0
                elif field_type == 'date':
                    template[field] = "2024-10-11T00:00:00"
                elif field_type == 'object':
                    template[field] = build_template(details.get('properties', {}))
                elif field_type == 'nested':
                    template[field] = [build_template(details.get('properties', {}))]
                else:
                    template[field] = "示例值"
        return [template]

    template = build_template(properties)
    return template

# 将生成的导入脚本保存为文件
def save_script(script, file_name):
    with open(file_name, 'w', encoding='utf-8') as f:
        f.write(script)
    print(f"导入脚本已生成: {file_name}")

# 保存数据模板为 JSON 文件
def save_template(template, file_name):
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(template, f, indent=4, ensure_ascii=False)
    print(f"数据模板已生成: {file_name}")

# 主程序
def main():
    # 获取索引的 mapping
    mapping = get_mapping(INDEX_NAME)
    if mapping:
        # 根据 mapping 生成导入脚本
        script = generate_import_script(mapping, INDEX_NAME)
        # 保存导入脚本为新的 .py 文件
        save_script(script, OUTPUT_SCRIPT)

        # 生成并保存数据模板
        template = generate_data_template(mapping, INDEX_NAME)
        save_template(template, OUTPUT_TEMPLATE)

if __name__ == '__main__':
    main()
