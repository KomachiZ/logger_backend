from flask import Flask, request, jsonify, send_file
import json
from concurrent.futures import ThreadPoolExecutor
import queue
import pandas as pd
import os
from datetime import datetime, timedelta
import pyarrow.parquet as pq
from pyarrow import Table
import threading

# 配置文件相关参数
CONFIG_FILE_PATH = './users_config.json'  # 用户配置文件路径

# Flask应用初始化
app = Flask(__name__)

# 线程池配置
executor = ThreadPoolExecutor(5)  # 设置线程池大小

# 主题队列配置
topics_queues = {
    'Themes': queue.Queue(),
    'topic2': queue.Queue(),
    'base': queue.Queue()
}

# 配置文件最后修改时间记录
config_last_modified = 0

def load_user_config():
    """
    加载用户配置文件，并检查文件是否已更新
    """
    global config_last_modified
    try:
        current_mtime = os.path.getmtime(CONFIG_FILE_PATH)
        if current_mtime > config_last_modified:
            with open(CONFIG_FILE_PATH, 'r') as file:
                config = json.load(file)
            config_last_modified = current_mtime
            print("Config file reloaded.")
            return config
    except FileNotFoundError as e:
        print(f"Config file not found: {e}")
    except Exception as e:
        print(f"Error loading config file: {e}")
    return None

def validate_user(username):
    """
    验证用户是否有效
    """
    config = load_user_config()
    if config is None:
        return username in app.config.get('valid_users', [])
    else:
        app.config['valid_users'] = config.get('valid_users', [])
        return username in app.config['valid_users']

#nginx探活
@app.route('/', methods=['GET'])
def index():
    return jsonify({"status": "ok", "message": "Server is running"}), 200

@app.route('/validate_user', methods=['POST'])
def validate_user_handler():
    """
    用户验证接口处理函数
    """
    username = request.json.get('username')
    if not username:
        return jsonify({"error": "Username is required"}), 400
    if validate_user(username):
        return jsonify({"message": "User validated"}), 200
    else:
        return jsonify({"error": "Invalid user"}), 403

def process_data(topic, data):
    """
    处理接收到的数据并存储
    """
    try:
        date_str = datetime.now().strftime('%Y-%m-%d')
        directory = f'./data/{topic}/{date_str}'
        filename = f'{directory}/{date_str}.json'
        os.makedirs(directory, exist_ok=True)

        with open(filename, 'a', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False)
            file.write('\n')
    except FileNotFoundError as e:
        print(f"File not found error while processing data for topic '{topic}': {e}")
    except OSError as e:
        print(f"OS error while processing data for topic '{topic}': {e}")
    except Exception as e:
        print(f"Unexpected error while processing data for topic '{topic}': {e}")

def compress_to_parquet(topic):
    """
    将JSON数据压缩转换为Parquet格式
    """
    try:
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        source_directory = f'./data/{topic}/{yesterday_str}'
        source_filename = f'{source_directory}/{yesterday_str}.json'
        target_filename = f'{source_directory}/{yesterday_str}.parquet'

        if os.path.exists(source_filename):
            df = pd.read_json(source_filename, lines=True)
            table = Table.from_pandas(df)
            pq.write_table(table, target_filename)
            #os.remove(source_filename)
    except FileNotFoundError as e:
        print(f"File not found error while compressing to parquet for topic '{topic}': {e}")
    except OSError as e:
        print(f"OS error while compressing to parquet for topic '{topic}': {e}")
    except Exception as e:
        print(f"Unexpected error while compressing to parquet for topic '{topic}': {e}")

def queue_worker(topic):
    """
    队列工作器函数，处理特定主题的数据队列
    """
    while True:
        try:
            data = topics_queues[topic].get()
            process_data(topic, data)
            topics_queues[topic].task_done()
        except Exception as e:
            print(f"Error in queue worker for topic '{topic}': {e}")

def schedule_compression():
    """
    定时压缩任务调度函数
    """
    try:
        for topic in topics_queues.keys():
            compress_to_parquet(topic)
        threading.Timer(86400, schedule_compression).start()  # 每24小时执行一次
    except Exception as e:
        print(f"Error in scheduling compression: {e}")

@app.route('/template/<filename>', methods=['GET'])
def get_template(filename):
    """
    模板文件获取接口
    """
    try:
        template_path = os.path.join(app.root_path, 'templates', filename)
        if os.path.exists(template_path):
            if filename.endswith(('.docx', '.xlsx', '.pdf')):
                return send_file(template_path, as_attachment=True)
            else:
                with open(template_path, 'r') as file:
                    content = file.read()
                return content, 200
        else:
            return "Template not found", 404
    except Exception as e:
        print(f"Error fetching template '{filename}': {e}")
        return "Internal Server Error", 500

@app.route('/log', methods=['POST'])
def log_handler():
    """
    日志数据处理接口
    """
    try:
        datas = request.get_json(force=True)
        if not isinstance(datas, list):
            return jsonify({"error": "Expected a list of data"}), 400

        processed_count = 0
        error_count = 0
        errors = []

        for data in datas:
            topic = data.get('topic')
            if topic in topics_queues:
                topics_queues[topic].put(data)
                processed_count += 1
            else:
                error_count += 1
                errors.append(f"Topic '{topic}' not supported")

        if error_count > 0:
            return jsonify({
                "message": f"Processed {processed_count} items, with {error_count} errors",
                "errors": errors
            }), 207

        return jsonify({"message": f"All {processed_count} data items queued for processing"}), 202
    except Exception as e:
        print(f"Error in log handler: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

# 全局错误处理
@app.errorhandler(Exception)
def handle_exception(e):
    """
    捕获所有异常，返回统一的错误响应
    """
    print(f"Unexpected error: {e}")
    return jsonify({"error": "Internal Server Error", "message": str(e)}), 500

if __name__ == '__main__':
    """
    应用程序入口
    """
    try:
        # 初始化配置
        initial_config = load_user_config()
        if initial_config:
            app.config['valid_users'] = initial_config.get('valid_users', [])

        # 启动工作线程
        for topic in topics_queues.keys():
            executor.submit(queue_worker, topic)

        # 启动定时压缩任务
        schedule_compression()

        # 启动Flask应用
        app.run(debug=True, host="0.0.0.0", port=5000)
    except Exception as e:
        print(f"Error starting application: {e}")
