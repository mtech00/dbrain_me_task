from flask import Flask, jsonify, send_file
import json
import os

app = Flask(__name__)

# Path to the JSON file
json_file_path = '/app/scraped_products.json'

# Ensure the JSON file exists
if not os.path.exists(json_file_path):
    # Create an empty file if it doesn't exist
    with open(json_file_path, 'w') as file:
        json.dump([], file)


@app.route('/get-data', methods=['GET'])
def get_data():
    """
    Serve the JSON data from consumed_data.json.
    """
    try:
        # Read the JSON file
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        # Return the data as JSON response
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/download-data', methods=['GET'])
def download_data():
    """
    Serve the JSON file as a downloadable file.
    """
    try:
        # Send the file for download
        return send_file(json_file_path, as_attachment=True), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)