import aiohttp
import asyncio
import json
import re
from bs4 import BeautifulSoup
from tqdm import tqdm

URL_MEF = 'https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx'
provincias= ['0101', '0102', '0103', '0104', '0105', '0106', '0107', '0201', '0202', '0203', '0204', '0205', '0206', '0207', '0208', '0209', '0210', '0211', '0212', '0213', '0214', '0215', '0216', '0217', '0218', '0219', '0220', '0301', '0302', '0303', '0304', '0305', '0306', '0307', '0401', '0402', '0403', '0404', '0405', '0406', '0407', '0408', '0501', '0502', '0503', '0504', '0505', '0506', '0507', '0508', '0509', '0510', '0511', '0601', '0602', '0603', '0604', '0605', '0606', '0607', '0608', '0609', '0610', '0611', '0612', '0613', '0701', '0801', '0802', '0803', '0804', '0805', '0806', '0807', '0808', '0809', '0810', '0811', '0812', '0813', '0901', '0902', '0903', '0904', '0905', '0906', '0907', '1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010', '1011', '1101', '1102', '1103', '1104', '1105', '1201', '1202', '1203', '1204', '1205', '1206', '1207', '1208', '1209', '1301', '1302', '1303', '1304', '1305', '1306', '1307', '1308', '1309', '1310', '1311', '1312', '1401', '1402', '1403', '1501', '1502', '1503', '1504', '1505', '1506', '1507', '1508', '1509', '1510', '1601', '1602', '1603', '1604', '1605', '1606', '1607', '1608', '1701', '1702', '1703', '1801', '1802', '1803', '1901', '1902', '1903', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2101', '2102', '2103', '2104', '2105', '2106', '2107', '2108', '2109', '2110', '2111', '2112', '2113', '2201', '2202', '2203', '2204', '2205', '2206', '2207', '2208', '2209', '2210', '2301', '2302', '2303', '2304', '2401', '2402', '2403', '2501', '2502', '2503', '2504']

distritosJson = {}

patron1 = r'^(\d+)-'
patron2 = r'-(\d+):'

async def fetch(session, url):
    async with session.get(url, ssl=False) as response:
        return await response.text()

async def procesarProvincia(p):
    departamento = p[:2]
    provincia = p[2:4]
    url = f'{URL_MEF}?_uhc=yes&0=&1=M&37=M&5={departamento}&6={provincia}&7=&y=2025&cpage=1&psize=400'
    
    async with aiohttp.ClientSession() as session:
        r = await fetch(session, url)
        soup = BeautifulSoup(r, 'lxml')

        htmlTabla = soup.find('table', {'class': 'Data'})
        if not htmlTabla:
            return {}

        htmlTabla2 = htmlTabla.findAll('td')
        if not htmlTabla2:
            return {}

        resultados = {}

        i = 0
        for td in htmlTabla2:
            valor = td.text.strip('\r\n').strip()
            if i in [0, 2, 3, 4, 5, 6, 7, 8]:
                i += 1                
                continue

            if i == 1:
                ubigeo = re.search(patron1, valor)
                codigo = re.search(patron2, valor)
                if ubigeo and codigo:
                    resultados[ubigeo.group(1)] = codigo.group(1)
            i += 1

            if i == 10:
                i = 0
        return resultados

async def main():
    tasks = [procesarProvincia(p) for p in provincias]

    with tqdm(total=len(tasks), desc='Procesando', unit=' provincia') as pbar:
        results = []
        for result in await asyncio.gather(*tasks):
            results.append(result)
            pbar.update(1)

    for result in results:
        distritosJson.update(result)

    with open('./json/distritosFinal.json', 'w', encoding='utf-8') as f:
        json.dump(distritosJson, f, indent=4, ensure_ascii=False)

    print('Datos guardados en: distritos.json.')

if __name__ == '__main__':
    asyncio.run(main())
