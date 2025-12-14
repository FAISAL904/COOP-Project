"""
Web Server for Data Quality Assessment Tool
Handles file uploads and serves the web interface
"""

from flask import Flask, render_template, request, jsonify
import os
from werkzeug.utils import secure_filename
import numpy as np
import json
from datetime import datetime
from data_quality import load_data, assess_data_quality

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['ALLOWED_EXTENSIONS'] = {'csv', 'xlsx', 'xls', 'json'}

# Create necessary directories
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs('reports', exist_ok=True)  # For saving reports permanently

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def convert_nan_to_none(obj):
    """Recursively convert NaN values to None for JSON serialization"""
    if isinstance(obj, dict):
        return {key: convert_nan_to_none(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_nan_to_none(item) for item in obj]
    elif isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None
    elif isinstance(obj, (np.integer, np.floating)):
        return obj.item() if not (np.isnan(obj) or np.isinf(obj)) else None
    else:
        return obj


@app.route('/')
def index():
    # Serve the HTML file from the root directory
    try:
        with open('index.html', 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        # Fallback to template if index.html doesn't exist
        return render_template('index.html')


@app.route('/evaluate', methods=['POST'])
def evaluate():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    # Check file extension, but allow files without extension (we'll detect type from content)
    if file.filename and '.' in file.filename:
        if not allowed_file(file.filename):
            return jsonify({'error': 'File type not allowed. Please upload CSV, Excel, or JSON files.'}), 400
    
    file_path = None
    try:
        # Save the uploaded file
        original_filename = file.filename or 'uploaded_file'
        filename = secure_filename(original_filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        # Load the data (pass original filename to help detect extension)
        df = load_data(file_path, filename, original_filename)
        
        # Check if dataframe is empty
        if df.empty:
            raise ValueError("The uploaded file is empty or contains no data")
        
        # Assess data quality
        quality_report = assess_data_quality(df)
        
        # Add preview data (first 20 rows) to the report
        # Replace NaN values with empty string for preview
        preview_df = df.head(20).fillna('')
        preview_data = preview_df.to_dict('records')
        
        # Convert any remaining NaN/Inf values to None for JSON serialization
        preview_data = convert_nan_to_none(preview_data)
        quality_report['preview_data'] = preview_data
        quality_report['preview_columns'] = list(df.columns)
        
        # Save report permanently
        report_filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{secure_filename(original_filename)}.json"
        report_path = os.path.join('reports', report_filename)
        
        # Prepare report for saving (convert all NaN to None)
        report_to_save = convert_nan_to_none(quality_report.copy())
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report_to_save, f, indent=2, ensure_ascii=False)
        
        quality_report['saved_report'] = report_filename
        
        # Clean up the uploaded file
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        
        # Convert NaN values in quality_report to None for JSON response
        quality_report = convert_nan_to_none(quality_report)
        
        return jsonify(quality_report)
    
    except Exception as e:
        # Clean up in case of error
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5001)
