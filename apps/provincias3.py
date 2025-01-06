import psycopg2
import requests
import urllib3
from bs4 import BeautifulSoup
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import time

urlMEF = 'https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx'
urllib3.disable_warnings()

def logError(mensaje, archivo='./logs/errorProvincias.log'):
	with open(archivo, 'a', encoding='utf-8') as f:
		f.write(mensaje + '\n')

def fetch(url):
    r = requests.get(url, verify=False)
    return r.text

def procesarURL(ubigeo, gasto, ap, a):
    departamento = ubigeo[:2]
    provincia = ubigeo[2:4]
    url = f'{urlMEF}?_uhc=yes&0=&1=M&37=M&5={departamento}&6={provincia}&{gasto}&y={a}&ap={ap}&cpage=1&psize=400'

    r = fetch(url)
    soup = BeautifulSoup(r, 'lxml')

    htmlTabla = soup.find('table', {'class': 'Data'})
    if not htmlTabla:
        logError(f"No se encontró la tabla para MEF('{ubigeo}', '{gasto}', '{ap}', '{a}') {url}")
        return {}

    htmlTabla2 = htmlTabla.findAll('td')
    if not htmlTabla2:
        logError(f"No se encontró la tabla(td) para MEF('{ubigeo}', '{gasto}', '{ap}', '{a}') {url}")
        return {}

    columnas = 10
    filaActual = []
    filas = []
    i = 0

    for td in htmlTabla2:
        valor = td.text.strip('\r\n').strip()

        if i == 0:
            i += 1
            continue

        if i == 1:
            if gasto == '30=':
                partes1 = valor[:4]
                partes2 = valor[6:]
            elif gasto == '8=':
                partes1 = valor[:2]
                partes2 = valor[4:]

            filaActual.append(partes1)
            filaActual.append(partes2)
        elif i >= 2 or i <= 8:
            montos = valor.replace(',', '')
            montos = montos.strip()
            if len(montos) == 0:
                filaActual.append(0)
            else:
                filaActual.append(montos)
        elif i == 9:
            avance = valor.strip()
            if len(avance) == 0:
                filaActual.append(0)
            else:
                filaActual.append(avance)
        else:
            filaActual.append(valor)

        i += 1

        if i == columnas:
            filaActual.append('M')
            filaActual.append(ubigeo)

            if gasto == '30=':  # Categoria presupuestal: c, Producto/Proyecto: p, funcion: f
                filaActual.append('c')
            elif gasto == '8=':
                filaActual.append('f')

            if ap == '':  # Actividad/Proyecto: 1, Proyecto: 2, Actividad: 3
                filaActual.append('1')
            elif ap == 'Proyecto':
                filaActual.append('2')
            elif ap == 'Actividad':
                filaActual.append('3')
            filaActual.append(a)
            filas.append(filaActual)
            filaActual = []
            i = 0

    try:
        conexion = psycopg2.connect(
            host='localhost',
            database='geoperu', 
            port='5433',
            user='postgres',
            password='123456',
            options='-c client_encoding=utf8'
        )

        cursor = conexion.cursor()
        query = '''
        INSERT INTO mef.consulta_ejecucion_gasto
        (codigo, descri, pia, pim, certi, comp_anua, eje_aten_comp_men, devengado, girado, avance, nivel_gobierno, ubigeo, tipo_gasto, acti_proy, anio)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        cursor.executemany(query, filas)
        conexion.commit()

        cursor.close()
        conexion.close()
    except Exception as e:
        logError(f"Error al insertar en la base de datos: {e}")

    return filas  # Devolver las filas para actualizarlas si es necesario

def main():
    provincias = ['0101', '0102', '0103', '0104', '0105', '0106', '0107', '0201', '0202', '0203', '0204', '0205', '0206', '0207', '0208', '0209', '0210', '0211', '0212', '0213', '0214', '0215', '0216', '0217', '0218', '0219', '0220', '0301', '0302', '0303', '0304', '0305', '0306', '0307', '0401', '0402', '0403', '0404', '0405', '0406', '0407', '0408', '0501', '0502', '0503', '0504', '0505', '0506', '0507', '0508', '0509', '0510', '0511', '0601', '0602', '0603', '0604', '0605', '0606', '0607', '0608', '0609', '0610', '0611', '0612', '0613', '0701', '0801', '0802', '0803', '0804', '0805', '0806', '0807', '0808', '0809', '0810', '0811', '0812', '0813', '0901', '0902', '0903', '0904', '0905', '0906', '0907', '1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010', '1011', '1101', '1102', '1103', '1104', '1105', '1201', '1202', '1203', '1204', '1205', '1206', '1207', '1208', '1209', '1301', '1302', '1303', '1304', '1305', '1306', '1307', '1308', '1309', '1310', '1311', '1312', '1401', '1402', '1403', '1501', '1502', '1503', '1504', '1505', '1506', '1507', '1508', '1509', '1510', '1601', '1602', '1603', '1604', '1605', '1606', '1607', '1608', '1701', '1702', '1703', '1801', '1802', '1803', '1901', '1902', '1903', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2101', '2102', '2103', '2104', '2105', '2106', '2107', '2108', '2109', '2110', '2111', '2112', '2113', '2201', '2202', '2203', '2204', '2205', '2206', '2207', '2208', '2209', '2210', '2301', '2302', '2303', '2304', '2401', '2402', '2403', '2501', '2502', '2503', '2504']
    #anios = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    anios = [2024]
    enQueGasta = ['30=', '8=']
    actividadProyectos = ['', 'Proyecto', 'Actividad']

    totalTareas = len(provincias) * len(enQueGasta) * len(actividadProyectos) * len(anios)
    tiempoInicial = time.time()

    try:
        with ProcessPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(procesarURL, ubigeo, gasto, ap, a)
                for ubigeo in provincias
                for gasto in enQueGasta
                for ap in actividadProyectos
                for a in anios
            ]

            for _ in tqdm(as_completed(futures), total=totalTareas, desc='Procesando'):
                pass

    except Exception as e:
        logError(f"Error: {e}")

    tiempoFinal = time.time()  # Finaliza la medición del tiempo
    totalTiempo = (tiempoFinal - tiempoInicial) / 60
    print(f'Tiempo total de ejecución: {totalTiempo:.2f} minutos')

if __name__ == '__main__':
    main()
