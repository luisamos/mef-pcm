import requests, json
from datetime import datetime
from bs4 import BeautifulSoup

anio= anio= datetime.now().year
URL_MEF= 'https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx'
pliegos= [440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465]

enQueGasta = ['30=', '8=']
actividadProyectos = ['', 'Proyecto', 'Actividad']

def logError(mensaje, archivo='./logs/errorDepartamentos.log'):
	with open(archivo, 'a', encoding='utf-8') as f:
		f.write(mensaje + '\n')

for pliego in pliegos:
    filas = []
    for gasto in enQueGasta:
        for ap in actividadProyectos:
            if ap == '':
                url = f'{URL_MEF}?_uhc=yes&0=&1=R&2=99&3={pliego}&{gasto}&y={anio}&cpage=1&psize=400&of=col2&od=1'
            else:
                url = f'{URL_MEF}?_uhc=yes&0=&1=R&2=99&3={pliego}&{gasto}&y={anio}&ap={ap}&cpage=1&psize=400&of=col2&od=1'

            r = requests.get(url)
            soup = BeautifulSoup(r.text, 'lxml')

            htmlTabla = soup.find('table', {'class': 'Data'})
            if not htmlTabla:
                logError(f"No se encontró la tabla para el pliego {pliego}, gasto {gasto}, ap {ap}: {url}")
                continue

            htmlTabla2 = htmlTabla.findAll('td')
            if not htmlTabla2:
                logError(f"No se encontró el td para el pliego {pliego}, gasto {gasto}, ap {ap}: {url}")
                continue

            columnas = 10
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
                    if ap == '':
                        filaActual.append('1')
                    elif ap == 'Proyecto':
                        filaActual.append('2')
                    elif ap == 'Actividad':
                        filaActual.append('3')
                 
                    filas.append(filaActual)
                    filaActual = []
                    i = 0
        
    inversiones=[]
    for fila in filas:
        inversiones.append({
            'a': f'{anio}',
            'g': 'R',
            'op': fila[0],
            'c': fila[1],            
            'p': fila[2],
            'd': fila[3],
            'i': fila[4],
            't': fila[5]        
        })

    with open(f'json/{pliego}.json', 'w', encoding='utf-8') as x:
        json.dump(inversiones, x, indent=4, ensure_ascii=False)
    print(f'Datos guardados en: {pliego}.json.')