from flask import Flask, request, jsonify, render_template
import sqlite3
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

@app.route('/metric1', methods=['GET'])
def get_metric1():
    try:
        # Conectar a la base de datos SQLite (asegúrate de tenerla en el directorio de tu proyecto o proporciona una ruta)
        connection = sqlite3.connect('company.db')
        cursor = connection.cursor()

        # Realizar la consulta SQL
        consulta = """
                SELECT Dept, Job, 
                count(Q1) as Q1,
                count(Q2) as Q2, 
                count(Q3) as Q3, 
                count(Q4) as Q4
                FROM(
                SELECT B.department as Dept, C.job as Job, date(a.hired_date) as Fec_ing, strftime('%Y', a.hired_date) AS año, strftime('%m', a.hired_date) AS mes, 
                CASE when strftime('%m', a.hired_date) in ('01','02','03') then 1 ELSE NULL END AS Q1,
                CASE when strftime('%m', a.hired_date) in ('04','05','06') then 1 ELSE NULL END AS Q2,
                CASE when strftime('%m', a.hired_date) in ('07','08','09') then 1 ELSE NULL END AS Q3,
                CASE when strftime('%m', a.hired_date) in ('10','11','12') then 1 ELSE NULL END AS Q4
                FROM employees A 
                INNER JOIN departments B ON A.department_id = B.id
                INNER JOIN jobs C ON A.job_id = C.id
                WHERE strftime('%Y', a.hired_date) = '2021')
                GROUP by Dept, Job
                ORDER BY Dept, Job
            """

        cursor.execute(consulta)
        resultados = cursor.fetchall()

        # Cerrar la conexión a la base de datos
        connection.close()

        # Formatear los resultados y devolverlos como JSON
        respuesta = []
        for fila in resultados:
            dept, job, q1, q2, q3, q4 = fila
            respuesta.append({
                'Dept': dept,
                'Job': job,
                'Q1': q1,
                'Q2': q2,
                'Q3': q3,
                'Q4': q4
            })

        return jsonify(respuesta)
        #return render_template('metrica1.html', resultados=respuesta)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/metric2', methods=['GET'])
def get_metric2():
    try:
        # Conectar a la base de datos SQLite (asegúrate de tenerla en el directorio de tu proyecto o proporciona una ruta)
        connection = sqlite3.connect('company.db')
        cursor = connection.cursor()

        # Realizar la consulta SQL
        consulta = """
                select e.id, e.department, e.conteo as hired from (select b.id as id, b.department as department, d.media as media , count(a.id) as conteo 
                from (select año, avg(conteo) as media from (SELECT b.id, b.department, strftime('%Y', a.hired_date) AS año, count(a.id) as conteo
                FROM employees A 
                INNER JOIN departments B ON A.department_id = B.id
                INNER JOIN jobs C ON A.job_id = C.id
                WHERE strftime('%Y', a.hired_date) = '2021'
                group by b.id, b.department, strftime('%Y', a.hired_date)) as c
                group by año) as d
                INNER join employees a on strftime('%Y', a.hired_date)=d.año
                inner JOIN departments b on b.id=a.department_id
                group by b.id, b.department, d.media) as e
                where e.conteo>e.media
                order by e.conteo desc 
            """

        cursor.execute(consulta)
        resultados = cursor.fetchall()

        # Cerrar la conexión a la base de datos
        connection.close()

        # Formatear los resultados y devolverlos como JSON
        respuesta = []
        for fila in resultados:
            id, department, hired = fila
            respuesta.append({
                'Id': id,
                'Department': department,
                'Hired': hired
            })

        return jsonify(respuesta)
        #return render_template('metrica1.html', resultados=respuesta)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)

#Para ejecutar: python app.py desde la terminal
#Para ingestar datos: curl -X POST -F "file=@departments.csv" -F "table_name=departments" http://localhost:5000/upload
