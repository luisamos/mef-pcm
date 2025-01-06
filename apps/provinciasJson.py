import os
import json
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configuración inicial para deshabilitar advertencias y establecer el año actual
anio = datetime.now().year
URL_MEF = 'https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx'
urllib3.disable_warnings()

def logError(mensaje, archivo='./logs/errorProvincias.log'):
	with open(archivo, 'a', encoding='utf-8') as f:
		f.write(mensaje + '\n')

# Función para obtener el contenido HTML desde una URL
def fetch(url):
    # Realiza una solicitud HTTP GET a la URL proporcionada
    r = requests.get(url, verify=False)
    return r.text

# Procesa una URL y extrae información relevante desde la tabla HTML
def procesarURL(ubigeo, gasto, ap):
    # Extraer el código de departamento y provincia desde el ubigeo
    departamento = ubigeo[:2]
    provincia = ubigeo[2:4]

    # Construir la URL según los parámetros proporcionados
    if ap == '':
        url = (f'{URL_MEF}?_uhc=yes&0=&1=M&37=M&5={departamento}&6={provincia}&{gasto}&y={anio}&cpage=1&psize=400&of=col2&od=1')
    else:
        url = (f'{URL_MEF}?_uhc=yes&0=&1=M&37=M&5={departamento}&6={provincia}&{gasto}&y={anio}&ap={ap}&cpage=1&psize=400&of=col2&od=1')

    try:
        # Obtener el contenido de la página web
        r = fetch(url)
        soup = BeautifulSoup(r, 'lxml')

        # Encontrar la tabla con clase 'Data'
        htmlTabla = soup.find('table', {'class': 'Data'})
        if not htmlTabla:
            logError(f"No se encontró la tabla MEF('{ubigeo}', '{gasto}', '{ap}') {url}")
            return

        htmlTabla2 = htmlTabla.findAll('td')
        if not htmlTabla2:
            logError(f"No se encontró el td MEF('{ubigeo}', '{gasto}', '{ap}') {url}")
            return

        columnas = 10  # Definir el número de columnas esperadas
        filas = []  # Lista para almacenar todas las filas procesadas
        filaActual = []  # Almacena la fila actual durante el procesamiento
        i = 0  # Contador de columnas

        for td in htmlTabla2:
            valor = td.text.strip('\r\n').strip()

            # Omitir columnas no relevantes
            if i in [0, 2, 4, 5, 6]:
                i += 1
                continue

            # Procesar la columna 1 para tipo de gasto
            if i == 1:
                if gasto == '30=':
                    partes1 = valor[:4]  # Tomar los primeros 4 caracteres
                    filaActual.append('c')
                elif gasto == '8=':
                    partes1 = valor[:2]  # Tomar los primeros 2 caracteres
                    filaActual.append('f')
                filaActual.append(partes1)

            # Procesar columnas de montos numéricos
            elif i in [3, 7, 8]:
                montos = valor.replace(',', '')
                montos = montos.strip()
                filaActual.append(float(montos) if montos else 0)

            i += 1

            # Cuando se completa una fila, agregarla a la lista
            if i == columnas:
                filaActual.append({'': '1', 'Proyecto': '2', 'Actividad': '3'}.get(ap, '0'))
                filas.append(filaActual)
                filaActual = []
                i = 0

        return filas  # Retornar todas las filas procesadas
    except Exception as e:
        logError(f"Error procesando MEF('{ubigeo}', '{gasto}', '{ap}') {e}, {url}")
        return []

# Convierte datos procesados a formato JSON y los guarda en un archivo
def array2json(filas, ubigeo):
    ruta_json = f'json/{ubigeo}.json'

    try:
        # Crear el directorio si no existe
        os.makedirs('json', exist_ok=True)

        # Cargar datos existentes si el archivo ya existe
        if os.path.exists(ruta_json):
            with open(ruta_json, 'r', encoding='utf-8') as archivo:
                try:
                    datos_existentes = json.load(archivo)
                except Exception:
                    datos_existentes = []
        else:
            datos_existentes = []

        # Agregar nuevas filas, asegurando que no haya duplicados
        for fila in filas:
            nueva_fila = {
                'a': f'{anio}',
                'g': 'M',
                'op': fila[0],
                'c': fila[1],
                'p': fila[2],
                'd': fila[3],
                'i': fila[4],
                't': fila[5]
            }
            if nueva_fila not in datos_existentes:
                datos_existentes.append(nueva_fila)

        # Ordenar las filas antes de guardar
        datos_existentes = sorted(datos_existentes, key=lambda x: (x['t'], x['c']))

        # Guardar los datos en el archivo JSON
        with open(ruta_json, 'w', encoding='utf-8') as archivo:
            json.dump(datos_existentes, archivo, indent=4, ensure_ascii=False)

    except PermissionError as e:
        logError(f"Error de permisos al escribir el archivo {ruta_json}: {e}")
    except Exception as e:
        logError(f"Error al guardar datos en {ruta_json}: {e}")

# Registra errores en un archivo de logs
def logError(mensaje, archivo='./logs/errorProvincias.log'):
    # Crear el directorio de logs si no existe
    os.makedirs(os.path.dirname(archivo), exist_ok=True)
    with open(archivo, 'a', encoding='utf-8') as f:
        f.write(mensaje + '\n')

# Procesa completamente una provincia antes de guardar el JSON
def procesarProvincia(ubigeo, enQueGasta, actividadProyectos):
    filas_totales = []  # Lista para acumular todas las filas de esta provincia
    for gasto in enQueGasta:
        for ap in actividadProyectos:
            filas = procesarURL(ubigeo, gasto, ap)  # Obtener filas para la combinación
            filas_totales.extend(filas)  # Agregar las filas procesadas
    array2json(filas_totales, ubigeo)  # Guardar los datos procesados en un archivo JSON

# Función principal para gestionar el procesamiento
def main():
    provincias= ['0101', '0102', '0103', '0104', '0105', '0106', '0107', '0201', '0202', '0203', '0204', '0205', '0206', '0207', '0208', '0209', '0210', '0211', '0212', '0213', '0214', '0215', '0216', '0217', '0218', '0219', '0220', '0301', '0302', '0303', '0304', '0305', '0306', '0307', '0401', '0402', '0403', '0404', '0405', '0406', '0407', '0408', '0501', '0502', '0503', '0504', '0505', '0506', '0507', '0508', '0509', '0510', '0511', '0601', '0602', '0603', '0604', '0605', '0606', '0607', '0608', '0609', '0610', '0611', '0612', '0613', '0701', '0801', '0802', '0803', '0804', '0805', '0806', '0807', '0808', '0809', '0810', '0811', '0812', '0813', '0901', '0902', '0903', '0904', '0905', '0906', '0907', '1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010', '1011', '1101', '1102', '1103', '1104', '1105', '1201', '1202', '1203', '1204', '1205', '1206', '1207', '1208', '1209', '1301', '1302', '1303', '1304', '1305', '1306', '1307', '1308', '1309', '1310', '1311', '1312', '1401', '1402', '1403', '1501', '1502', '1503', '1504', '1505', '1506', '1507', '1508', '1509', '1510', '1601', '1602', '1603', '1604', '1605', '1606', '1607', '1608', '1701', '1702', '1703', '1801', '1802', '1803', '1901', '1902', '1903', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2101', '2102', '2103', '2104', '2105', '2106', '2107', '2108', '2109', '2110', '2111', '2112', '2113', '2201', '2202', '2203', '2204', '2205', '2206', '2207', '2208', '2209', '2210', '2301', '2302', '2303', '2304', '2401', '2402', '2403', '2501', '2502', '2503', '2504']
    enQueGasta = ['30=', '8=']  # Tipos de gastos a procesar
    actividadProyectos = ['', 'Proyecto', 'Actividad']  # Tipos de actividades o proyectos

    try:
        # Procesar provincias en paralelo utilizando un ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = {
                executor.submit(procesarProvincia, ubigeo, enQueGasta, actividadProyectos): ubigeo
                for ubigeo in provincias
            }

            # Monitorear el progreso de las tareas
            for future in tqdm(as_completed(futures), total=len(provincias), desc='Procesando provincias'):
                ubigeo = futures[future]
                try:
                    future.result()  # Verificar el resultado de la tarea
                except Exception as e:
                    logError(f"Error general procesando provincia '{ubigeo}': {e}")

    except Exception as e:
        logError(f"Error general: {e}")

# Punto de entrada principal del programa
if __name__ == '__main__':
    main()
