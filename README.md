Instrucciones:

1. Crear el entorno virtual con python 3.11:
   C:\Python311\python.exe -m venv venv

2. Ingresar al entorno virtual:
   .\venv\Scripts\activate

3. Instalar las depedencias en el entorno virtual:
   pip install -r requirements.txt
4. Ejecutar playwright:
   playwright install

5. Ejecutar el archivo SQL en la base de datos PostgreSQL:
   (Windows)
   psql -d mef -U postgres -f consulta_ejecucion_gasto.sql

   (Linux)
   su postgres
   psql -d mef -f consulta_ejecucion_gasto.sql

6. Configurar la cadena de conexión de los archivos departamentos.py, provincias3.py, distritos3.py y ejecutar:
   cd .\apps
   python departamentos.py
