import psycopg2
import requests
import urllib3
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import csv
import os

# Deshabilitar advertencias SSL
urllib3.disable_warnings()
urlMEF = 'https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx'

def logError(mensaje, archivo='./apps/logs/errorProvincias.log'):
	with open(archivo, 'a', encoding='utf-8') as f:
		f.write(mensaje + '\n')

def fetch(url):
    r = requests.get(url, verify=False)  # Omitir la verificación SSL
    return r.text

def procesarURL(ubigeo, gasto, ap, a, filename):
    departamento = ubigeo[:2]
    provincia = ubigeo[2:4]
    url = f'{urlMEF}?_uhc=yes&0=&1=M&37=M&5={departamento}&6={provincia}&{gasto}&y={a}&ap={ap}&cpage=1&psize=400'

    try:
        r = fetch(url)
        soup = BeautifulSoup(r, 'lxml')

        htmlTabla = soup.find('table', {'class': 'Data'})
        if not htmlTabla:
            logError(f"No se encontró tabla en {url}")
            return

        filas_html = htmlTabla.find_all('tr')
        
        filas = []

        for fila_html in filas_html[1:]:  # Omitir la fila de encabezados
            celdas = fila_html.find_all('td')
            if len(celdas) < 10:
                logError(f"Celdas incompleta o mal formada: {url}")
                continue  # Ignorar filas incompletas

            codigo, descripcion = (celdas[1].text.split(': ', 1) + [''])[:2]
            codigo = codigo.strip()
            descripcion = descripcion.strip()

            valores = [
                int(c.text.replace(',', '').strip()) if c.text.strip() else 0
                for c in celdas[2:9]
            ]

            avance = celdas[9].text.strip() if celdas[9].text.strip() else '0.0'
            codigoGasto=''
            if gasto == '30=':
                codigoGasto = 'c'
            elif gasto == '8=':
                codigoGasto = 'f'

            if ap == '':
                codigoAP= '1'
            elif ap == 'Proyecto':
                codigoAP= '2'
            elif ap == 'Actividad':
                codigoAP= '3'
            
            fila = [
                codigo, descripcion, *valores, avance, 99, ubigeo, codigoGasto, codigoAP, a
            ]
            filas.append(fila)

        guardarCSV(filas, filename)

    except Exception as e:
        logError(f"Error procesando MEF('{ubigeo}', '{gasto}', '{ap}', '{a}') {e}")

def guardarCSV(filas, filename='datosMEF.csv'):
    '''
    Guarda las filas en un archivo CSV usando ';' como delimitador.
    Si el archivo no existe, agrega los encabezados.
    '''
    is_new_file = not os.path.exists(filename)
    
    with open(filename, mode='a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=';')  # Configurar delimitador como ';'
        
        if is_new_file:
            writer.writerow([
                'codigo', 'descri', 'pia', 'pim', 'certi', 'comp_anua',
                'eje_aten_comp_men', 'devengado', 'girado', 'avance',
                'ubigeo', 'tipo_gasto', 'acti_proy', 'anio'
            ])
        
        for fila in filas:
            writer.writerow(fila)

def csv2pgsql(filename, conexion):
    '''
    Carga los datos del archivo CSV a la base de datos PostgreSQL
    '''
    try:
        cursor = conexion.cursor()
        
        with open(filename, 'r', encoding='utf-8') as datos:
            cursor.copy_expert(
                '''
                COPY mef.consulta_ejecucion_gasto
                (codigo, descri, pia, pim, certi, comp_anua, eje_aten_comp_men,
                devengado, girado, avance, sector, ubigeo, tipo_gasto, acti_proy, anio)
                FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER TRUE)
                ''',
                datos
            )
        conexion.commit()
         # Obtener el número de registros insertados
        cursor.execute('SELECT COUNT(*) FROM mef.consulta_ejecucion_gasto')
        totalRegistros = cursor.fetchone()[0]
        
        print(f'{totalRegistros} registros insertados correctamente.')
    except Exception as e:
        print(f'Error al insertar datos en la base de datos: {e}')
        conexion.rollback()

def logError(mensaje, archivo='./logs/errorProvincias.log'):
    '''
    Registra errores en un archivo de texto.
    '''
    with open(archivo, 'a', encoding='utf-8') as f:
        f.write(mensaje + '\n')

def main():
    provincias = ['0101', '0102', '0103', '0104', '0105', '0106', '0107', '0201', '0202', '0203', '0204', '0205', '0206', '0207', '0208', '0209', '0210', '0211', '0212', '0213', '0214', '0215', '0216', '0217', '0218', '0219', '0220', '0301', '0302', '0303', '0304', '0305', '0306', '0307', '0401', '0402', '0403', '0404', '0405', '0406', '0407', '0408', '0501', '0502', '0503', '0504', '0505', '0506', '0507', '0508', '0509', '0510', '0511', '0601', '0602', '0603', '0604', '0605', '0606', '0607', '0608', '0609', '0610', '0611', '0612', '0613', '0701', '0801', '0802', '0803', '0804', '0805', '0806', '0807', '0808', '0809', '0810', '0811', '0812', '0813', '0901', '0902', '0903', '0904', '0905', '0906', '0907', '1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010', '1011', '1101', '1102', '1103', '1104', '1105', '1201', '1202', '1203', '1204', '1205', '1206', '1207', '1208', '1209', '1301', '1302', '1303', '1304', '1305', '1306', '1307', '1308', '1309', '1310', '1311', '1312', '1401', '1402', '1403', '1501', '1502', '1503', '1504', '1505', '1506', '1507', '1508', '1509', '1510', '1601', '1602', '1603', '1604', '1605', '1606', '1607', '1608', '1701', '1702', '1703', '1801', '1802', '1803', '1901', '1902', '1903', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2101', '2102', '2103', '2104', '2105', '2106', '2107', '2108', '2109', '2110', '2111', '2112', '2113', '2201', '2202', '2203', '2204', '2205', '2206', '2207', '2208', '2209', '2210', '2301', '2302', '2303', '2304', '2401', '2402', '2403', '2501', '2502', '2503', '2504']
    anios = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    enQueGasta = ['30=', '8=']
    actividadProyectos = ['', 'Proyecto', 'Actividad']

    totalTareas = len(provincias) * len(enQueGasta) * len(actividadProyectos) * len(anios)
    filename = 'datosMEF.csv'

    try:
        # Procesar todos los datos
        '''with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(procesarURL, ubigeo, gasto, ap, a, filename)
                for ubigeo in provincias
                for gasto in enQueGasta
                for ap in actividadProyectos
                for a in anios
            ]
            for _ in tqdm(as_completed(futures), total=totalTareas, desc='Procesando'):
                pass
        ''' 
        # Establecer conexión a la base de datos
        conexion = psycopg2.connect(
            host='localhost',
            database='geoperu',
            port='5432',
            user='postgres',
            password='123456',
            options='-c client_encoding=utf8'
        )
        print('Conexión exitosa')
        # Una vez que se han descargado todos los datos, cargarlos a la base de datos
        csv2pgsql(filename, conexion)

    except Exception as e:
        logError(f"Error general: {e}")
    finally:
        if conexion:
            conexion.close()
            print('Conexión cerrada')

if __name__ == '__main__':
    main()
