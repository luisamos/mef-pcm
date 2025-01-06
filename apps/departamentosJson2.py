import os
import json
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import urllib3

anio = datetime.now().year
URL_MEF = 'https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx'
urllib3.disable_warnings()

def logError(mensaje, archivo='./apps/logs/errorDepartamentos.log'):
	with open(archivo, 'a', encoding='utf-8') as f:
		f.write(mensaje + '\n')

def fetch(url):
    r = requests.get(url, verify=False)
    return r.text

def procesarURL(pliego, gasto, ap):
    if ap == '':
        url = f'{URL_MEF}?_uhc=yes&0=&1=R&2=99&3={pliego}&{gasto}&y={anio}&cpage=1&psize=400&of=col2&od=1'
    else:
        url = f'{URL_MEF}?_uhc=yes&0=&1=R&2=99&3={pliego}&{gasto}&y={anio}&ap={ap}&cpage=1&psize=400&of=col2&od=1'

    try:
        r = fetch(url)
        soup = BeautifulSoup(r, 'lxml')

        htmlTabla = soup.find('table', {'class': 'Data'})
        if not htmlTabla:
            logError(f"No se encontró la tabla para el pliego {pliego}('{gasto}', '{ap}') {url}")
            return

        htmlTabla2 = htmlTabla.findAll('td')
        if not htmlTabla2:
            logError(f"No se encontró el td para el pliego {pliego}('{gasto}', '{ap}') {url}")
            return

        columnas = 10
        filas = []
        filaActual = []
        i = 0

        for td in htmlTabla2:
            valor = td.text.strip('\r\n').strip()

            if i in [0, 2, 4, 5, 6]:
                i += 1
                continue

            if i == 1:
                if gasto == '30=':
                    partes1 = valor[:4]
                    filaActual.append('c')
                elif gasto == '8=':
                    partes1 = valor[:2]
                    filaActual.append('f')
                filaActual.append(partes1)

            elif i in [3, 7, 8]:
                montos = valor.replace(',', '')
                montos = montos.strip()
                filaActual.append(float(montos) if montos else 0)

            i += 1

            if i == columnas:
                filaActual.append({'': '1', 'Proyecto': '2', 'Actividad': '3'}.get(ap, '0'))
                filas.append(filaActual)
                filaActual = []
                i = 0

        return filas
    except Exception as e:
        logError(f"Error procesando MEF('{pliego}', '{gasto}', '{ap}'): {e}, {url}")
        return []

def array2json(filas, pliego, nivelGobierno):
    rutaJson = f'./json/dpto/{pliego}.json'

    try:
        os.makedirs('json', exist_ok=True)

        if os.path.exists(rutaJson):
            with open(rutaJson, 'r', encoding='utf-8') as archivo:
                try:
                    datosExistentes = json.load(archivo)
                except Exception:
                    datosExistentes = []
        else:
            datosExistentes = []

        for fila in filas:
            nueva_fila = {
                'a': f'{anio}',
                'g': nivelGobierno,
                'op': fila[0],
                'c': fila[1],
                'p': fila[2],
                'd': fila[3],
                'i': fila[4],
                't': fila[5]
            }
            if nueva_fila not in datosExistentes:
                datosExistentes.append(nueva_fila)

        datosExistentes = sorted(datosExistentes, key=lambda x: (x['t'], x['c']))

        with open(rutaJson, 'w', encoding='utf-8') as archivo:
            json.dump(datosExistentes, archivo, indent=4, ensure_ascii=False)

    except PermissionError as e:
        logError(f"Error de permisos al escribir el archivo {rutaJson}: {e}")
    except Exception as e:
        logError(f"Error al guardar datos en {rutaJson}: {e}")

def logError(mensaje, archivo='./logs/errorDepartamentos.log'):
    os.makedirs(os.path.dirname(archivo), exist_ok=True)
    with open(archivo, 'a', encoding='utf-8') as f:
        f.write(mensaje + '\n')

def procesarDepartamento(pliego, enQueGasta, actividadProyectos):
    filaSTotales = []
    for gasto in enQueGasta:
        for ap in actividadProyectos:
            filas = procesarURL(pliego, gasto, ap)
            filaSTotales.extend(filas)
    array2json(filaSTotales, pliego, 'R')

def limpiarArchivos():
    folderPath = './json/dpto'
    for fileName in os.listdir(folderPath):
        filePath = os.path.join(folderPath, fileName)
        try:
            if os.path.isfile(filePath) or os.path.islink(filePath):
                os.unlink(filePath)
            
        except Exception as e:
            logError(f"Error al eliminar {filePath}: {e}")

def main():
    pliegos= [440 ,441]#, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465]
    enQueGasta = ['30=', '8=']
    actividadProyectos = ['', 'Proyecto', 'Actividad']
    try:
        limpiarArchivos()    
        for pliego in pliegos:
            procesarDepartamento(pliego, enQueGasta, actividadProyectos)
    except Exception as e:
        logError(f"Error general: {e}")

if __name__ == '__main__':
    main()
