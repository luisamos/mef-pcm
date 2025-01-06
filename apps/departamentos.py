import psycopg2, requests, time, urllib3
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

URL_MEF = 'https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx'
urllib3.disable_warnings()
# https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx?_uhc=yes&0=&1=R&2=99&3=440&30=&y=2024&cpage=1&psize=400

def conectar(url):
	r = requests.get(url, verify=False)
	return r.text

def logError(mensaje, archivo='./logs/errorDepartamentos.log'):
	with open(archivo, 'a', encoding='utf-8') as f:
		f.write(mensaje + '\n')

def procesarURL(pliego, gasto, ap, a, conexion, ubigeo):
	if ap == '':
		url = f'{URL_MEF}?_uhc=yes&0=&1=R&2=99&3={pliego}&{gasto}&y={a}&cpage=1&psize=400'
	else:
		url = f'{URL_MEF}?_uhc=yes&0=&1=R&2=99&3={pliego}&{gasto}&y={a}&ap={ap}&cpage=1&psize=400'
	try:
		r = conectar(url)
		soup = BeautifulSoup(r, 'lxml')
		htmlTabla = soup.find('table', {'class': 'Data'})

		if not htmlTabla:
			logError(f"No se encontró la tabla para MEF('{pliego}', '{gasto}', '{ap}', '{a}', '{ubigeo}'): {url}")
			return

		htmlTabla2 = htmlTabla.findAll('td')

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
				montos = valor.replace(',', '').strip()
				filaActual.append(montos if montos else 0)
			elif i == 9:
				avance = valor.strip()
				filaActual.append(avance if avance else 0)
			else:
				filaActual.append(valor)

			i += 1
			if i == columnas:
				filaActual.append('R')
				filaActual.append(ubigeo)
				filaActual.append('c' if gasto == '30=' else 'f')
				filaActual.append({'': '1', 'Proyecto': '2', 'Actividad': '3'}[ap])
				filaActual.append(a)
				filas.append(filaActual)
				filaActual = []
				i = 0

		cursor = conexion.cursor()
		query = '''
			INSERT INTO mef.consulta_ejecucion_gasto
			(codigo, descri, pia, pim, certi, comp_anua, eje_aten_comp_men, devengado, girado, avance, nivel_gobierno, ubigeo, tipo_gasto, acti_proy, anio)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
		'''
		cursor.executemany(query, filas)
		conexion.commit()

	except Exception as e:
		logError(f"Error procesando {url}: {e}")
	finally:
		cursor.close()

def main():
	pliegos = [440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465]
	ubigeos = {440: '01', 441: '02', 442: '03', 443: '04', 444: '05', 445: '06', 464: '07', 446: '08', 447: '09', 448: '10', 449: '11', 450: '12', 451: '13', 452: '14', 463: '15', 465: '1501', 453: '16', 454: '17', 455: '18', 456: '19', 457: '20', 458: '21', 459: '22', 460: '23', 461: '24', 462: '25'}
	#anios = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
	anios = [2024]
	enQueGasta = ['30=', '8=']
	actividadProyectos = ['', 'Proyecto', 'Actividad']

	totalTareas = len(pliegos) * len(enQueGasta) * len(actividadProyectos) * len(anios)
	tiempoInicial = time.time()
	
	conexion = psycopg2.connect(
			host='localhost',
			database='geoperu',
			port='5433',
			user='postgres',
			password='123456',
			options='-c client_encoding=utf8'
		)
	try:
		with ThreadPoolExecutor(max_workers=2) as executor:
			futures = [
				executor.submit(procesarURL, pliego, gasto, ap, a, conexion, ubigeo=ubigeos.get(pliego))
				for pliego in pliegos
				for gasto in enQueGasta
				for ap in actividadProyectos
				for a in anios
			]

			for _ in tqdm(as_completed(futures), total=totalTareas, desc='Procesando'):
				pass

	except Exception as e:
		logError(f"Error: {e}")
	finally:
		conexion.close()
		tiempoFinal = time.time()
		totalTiempo = (tiempoFinal - tiempoInicial)/60
		print(f'Tiempo total de ejecución: {totalTiempo:.2f} minutos')

if __name__ == '__main__':
	main()