from flask import Flask, request, jsonify, send_file
import json
import os
from datetime import datetime

# Configuration
CONFIG_FILE_PATH = './users_config.json'
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB

app = Flask(__name__)
config_last_modified = 0

TOPICS = {'Themes', 'topic2', 'base'}

def ensure_directory(topic):
    """Ensure directory exists for given topic and date"""
    date_str = datetime.now().strftime('%Y-%m-%d')
    directory = f'./data/{topic}/{date_str}'
    os.makedirs(directory, exist_ok=True)
    return directory, date_str

def load_user_config():
    """Load and monitor user configuration with hot reload"""
    global config_last_modified
    try:
        if not os.path.exists(CONFIG_FILE_PATH):
            return None
        
        current_mtime = os.path.getmtime(CONFIG_FILE_PATH)
        if current_mtime > config_last_modified:
            with open(CONFIG_FILE_PATH, 'r') as file:
                config = json.load(file)
            config_last_modified = current_mtime
            app.config['valid_users'] = config.get('valid_users', [])
            return config
    except Exception as e:
        print(f"Config loading error: {e}")
        return None
    return None

def validate_user(username):
    """Validate user with hot reload support"""
    config = load_user_config()
    return username in app.config.get('valid_users', [])

def process_data(topic, data):
    """Process data and write to file"""
    try:
        directory, date_str = ensure_directory(topic)
        filename = f'{directory}/{date_str}.json'

        # Check file size
        current_size = os.path.getsize(filename) if os.path.exists(filename) else 0
        data_size = len(json.dumps(data).encode('utf-8'))
        
        if current_size + data_size > MAX_FILE_SIZE:
            print(f"File size limit exceeded for topic {topic}")
            return False

        # Write data
        with open(filename, 'a', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False)
            file.write('\n')
        return True
    except Exception as e:
        print(f"Data processing error for topic {topic}: {e}")
        return False

@app.route('/validate_user', methods=['POST'])
def validate_user_handler():
    username = request.json.get('username')
    if not username:
        return jsonify({"error": "Username is required"}), 400
    if validate_user(username):
        return jsonify({"message": "User validated"}), 200
    return jsonify({"error": "Invalid user"}), 403

@app.route('/template/<filename>', methods=['GET'])
def get_template(filename):
    try:
        template_path = os.path.join(app.root_path, 'templates', filename)
        if os.path.exists(template_path):
            if filename.endswith(('.docx', '.xlsx', '.pdf')):
                return send_file(template_path, as_attachment=True)
            with open(template_path, 'r') as file:
                return file.read(), 200
        return "Template not found", 404
    except Exception as e:
        print(f"Template error: {e}")
        return "Internal Server Error", 500

@app.route('/', methods=['GET'])
def index():
    return jsonify({"status": "ok", "message": "Server is running"}), 200

@app.route('/log', methods=['POST'])
def log_handler():
    try:
        datas = request.get_json(force=True)
        if not isinstance(datas, list):
            return jsonify({"error": "Expected a list of data"}), 400

        processed_count = 0
        error_count = 0
        errors = []

        for data in datas:
            topic = data.get('topic')
            if topic in TOPICS:
                if process_data(topic, data):
                    processed_count += 1
                else:
                    error_count += 1
                    errors.append(f"Processing failed for topic '{topic}'")
            else:
                error_count += 1
                errors.append(f"Topic '{topic}' not supported")

        if error_count > 0:
            return jsonify({
                "message": f"Processed {processed_count} items, with {error_count} errors",
                "errors": errors
            }), 207

        return jsonify({"message": f"All {processed_count} data items processed"}), 202

    except Exception as e:
        print(f"Log handler error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    print(f"Unexpected error: {e}")
    return jsonify({"error": "Internal Server Error", "message": str(e)}), 500

if __name__ == '__main__':
    try:
        # Initialize config
        initial_config = load_user_config()
        if initial_config:
            app.config['valid_users'] = initial_config.get('valid_users', [])

        # Ensure data directory exists
        os.makedirs('./data', exist_ok=True)
        
        # Start Flask app
        app.run(debug=True, host="0.0.0.0", port=5000)
    except Exception as e:
        print(f"Startup error: {e}")