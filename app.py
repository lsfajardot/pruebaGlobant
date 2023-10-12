from flask import Flask, request, jsonify
import pandas as pd
from sqlalchemy import create_engine

app = Flask(__name__)

# Configure your database connection
DATABASE_URI = "sqlite:///company.db"  # Change this to your database URI
engine = create_engine(DATABASE_URI)

@app.route('/upload', methods=['POST'])
def upload_data():
    try:
        # Check if a file is attached to the request
        if 'file' not in request.files:
            return jsonify({'error': 'No existe archivo CSV'}), 400

        file = request.files['file']

        if file.filename == '':
            return jsonify({'error': 'No se selecciono archivo'}), 400

        if file:
            df = pd.read_csv(file, header=None)

            # Check the name of the table
            table_name = request.form.get('table_name')  # Add table_name field to your request

            if table_name not in ['departments', 'jobs', 'employees']:
                return jsonify({'error': 'Nombre de Tabla NO valido'}), 400

            # Define column names explicitly (replace these with your actual column names)
            if table_name == 'departments':
                df.columns = ['id', 'department']
            elif table_name == 'jobs':
                df.columns = ['id', 'job']
            elif table_name == 'employees':
                df.columns = ['id', 'name', 'datetime', 'department_id', 'job_id']

            # Insert data into the database using SQLAlchemy
            df.to_sql(table_name, con=engine, if_exists='append', index=False)

            return jsonify({'message': 'Data cargada de manera exitosa'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

#Para ejecutar: python app.py desde la terminal
#Para ingestar datos: curl -X POST -F "file=@departments.csv" -F "table_name=departments" http://localhost:5000/upload
