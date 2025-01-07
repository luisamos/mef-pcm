import psycopg2
import time
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from playwright.sync_api import sync_playwright
import inspect

urlMEF = 'https://apps5.mineco.gob.pe/transparencia/Navegador/Navegar_7.aspx'
parametros = {
        'host': 'localhost',
        'database': 'pcm',
        'port': '5432',
        'user': 'postgres',
        'password': '123456',
        'options': '-c client_encoding=utf8'
    }
conexion = psycopg2.connect(**parametros)

def obtenerNombreVariable(variable):
    for nombre, valor in inspect.currentframe().f_back.f_locals.items():
        if valor is variable:
            return nombre

def FechaHoraActual():
    horaActual = time.time()
    fechaHora = datetime.fromtimestamp(horaActual)
    return fechaHora.strftime('%d/%m/%Y %H:%M')

def logCustom(mensaje, archivo='./logs/customDistritos.log'):
	with open(archivo, 'a', encoding='utf-8') as f:
		f.write(mensaje + '\n')

def logError(mensaje, archivo='./logs/errorDistritos.log'):
	with open(archivo, 'a', encoding='utf-8') as f:
		f.write(mensaje + '\n')

def procesarURL(ubigeo, gasto, ap, a, distrito, conexion, estado):
    departamento = ubigeo[:2]
    provincia = ubigeo[2:4]
    url = f'{urlMEF}?_uhc=yes&0=&1=M&37=M&5={departamento}&6={provincia}&7={distrito}&{gasto}&y={a}&ap={ap}&cpage=1&psize=400'
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)

        table = page.query_selector('table.Data')
        if table:
            columnas = 10
            filaActual = []
            filas = []
            i = 0
            cells = table.query_selector_all('td')
            if cells:
                for celda in cells:
                    valor = celda.inner_text()
                    
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
                    elif 2 <= i <= 8:
                        montos = valor.replace(',', '').strip()
                        filaActual.append(float(montos) if montos else 0)
                    elif i == 9:
                        avance = valor.strip()
                        filaActual.append(float(avance) if avance else 0)
                    i += 1

                    if i == columnas:
                        filaActual.append('M')
                        filaActual.append(ubigeo)
                        filaActual.append('c' if gasto == '30=' else 'f')
                        filaActual.append('1' if ap == '' else '2' if ap == 'Proyecto' else '3')
                        filaActual.append(a)
                        filas.append(filaActual)
                        filaActual = []
                        i = 0

                if not filas:
                    logError(f"0 filas a insertar MEF('{ubigeo}', '{gasto}', '{ap}', '{a}', '{distrito}') {FechaHoraActual()} {url}")
                    return

                try:       
                    cursor = conexion.cursor()
                    query = '''
                    INSERT INTO mef.consulta_ejecucion_gasto
                    (codigo, descri, pia, pim, certi, comp_anua, eje_aten_comp_men, devengado, girado, avance, nivel_gobierno, ubigeo, tipo_gasto, acti_proy, anio)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    cursor.executemany(query, filas)
                    if estado:
                        filasInsertadas = cursor.rowcount
                        logCustom(f"{filasInsertadas} filas registradas: '{ubigeo}', {FechaHoraActual()}, {url}")
                    conexion.commit()
                except Exception as e:
                    logError(f"Error insertar ubigeo' {ubigeo}' {FechaHoraActual()}: {e}, {url}")
                finally:
                    cursor.close()
            else:
                logError(f"Nulo tabla(td) {FechaHoraActual()} MEF('{ubigeo}', '{gasto}', '{ap}', '{a}', '{distrito}') {url}")
        else:
            logError(f"Nulo tabla {FechaHoraActual()} MEF('{ubigeo}', '{gasto}', '{ap}', '{a}', '{distrito}') {url}")

        browser.close()

def main(arregloDistritos):
    with open('./json/distritos.json', 'r') as archivo:
        datos = json.load(archivo)    

    anios = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    enQueGasta = ['30=', '8=']
    actividadProyectos = ['', 'Proyecto', 'Actividad']

    totalTareas = len(arregloDistritos) * len(enQueGasta) * len(actividadProyectos) * len(anios)

    try:
        with ThreadPoolExecutor(max_workers=7) as executor:
            futures = [
                executor.submit(procesarURL, ubigeo, gasto, ap, a, datos.get(ubigeo), conexion, False)
                for ubigeo in arregloDistritos
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
        
if __name__ == '__main__':
    distritos010203 = ['010101', '010102', '010103','010104', '010105', '010106', '010107', '010108', '010109', '010110', '010111', '010112', '010113', '010114', '010115', '010116', '010117', '010118', '010119', '010120', '010121', '010201', '010202', '010203', '010204', '010205', '010206', '010301', '010302', '010303', '010304', '010305', '010306', '010307', '010308', '010309', '010310', '010311', '010312', '010401', '010402', '010403', '010501', '010502', '010503', '010504', '010505', '010506', '010507', '010508', '010509', '010510', '010511', '010512', '010513', '010514', '010515', '010516', '010517', '010518', '010519', '010520', '010521', '010522', '010523', '010601', '010602', '010603', '010604', '010605', '010606', '010607', '010608', '010609', '010610', '010611', '010612', '010701', '010702', '010703', '010704', '010705', '010706', '010707', '020101', '020102', '020103', '020104', '020105', '020106', '020107', '020108', '020109', '020110', '020111', '020112', '020201', '020202', '020203', '020204', '020205', '020301', '020302', '020303', '020304', '020305', '020306', '020401', '020402', '020501', '020502', '020503', '020504', '020505', '020506', '020507', '020508', '020509', '020510', '020511', '020512', '020513', '020514', '020515', '020601', '020602', '020603', '020604', '020605', '020606', '020607', '020608', '020609', '020610', '020611', '020701', '020702', '020703', '020801', '020802', '020803', '020804', '020901', '020902', '020903', '020904', '020905', '020906', '020907', '021001', '021002', '021003', '021004', '021005', '021006', '021007', '021008', '021009', '021010', '021011', '021012', '021013', '021014', '021015', '021016', '021101', '021102', '021103', '021104', '021105', '021201', '021202', '021203', '021204', '021205', '021206', '021207', '021208', '021209', '021210', '021301', '021302', '021303', '021304', '021305', '021306', '021307', '021308', '021401', '021402', '021403', '021404', '021405', '021406', '021407', '021408', '021409', '021410', '021501', '021502', '021503', '021504', '021505', '021506', '021507', '021508', '021509', '021510', '021511', '021601', '021602', '021603', '021604', '021701', '021702', '021703', '021704', '021705', '021706', '021707', '021708', '021709', '021710', '021801', '021802', '021803', '021804', '021805', '021806', '021807', '021808', '021809', '021901', '021902', '021903', '021904', '021905', '021906', '021907', '021908', '021909', '021910', '022001', '022002', '022003', '022004', '022005', '022006', '022007', '022008', '030101', '030102', '030103', '030104', '030105', '030106', '030107', '030108', '030109', '030201', '030202', '030203', '030204', '030205', '030206', '030207', '030208', '030209', '030210', '030211', '030212', '030213', '030214', '030215', '030216', '030217', '030218', '030219', '030220', '030301', '030302', '030303', '030304', '030305', '030306', '030307', '030401', '030402', '030403', '030404', '030405', '030406', '030407', '030408', '030409', '030410', '030411', '030412', '030413', '030414', '030415', '030416', '030417', '030501', '030502', '030503', '030504', '030505', '030506', '030601', '030602', '030603', '030604', '030605', '030606', '030607', '030608', '030609', '030610', '030611', '030612', '030701', '030702', '030703', '030704', '030705', '030706', '030707', '030708', '030709', '030710', '030711', '030712', '030713', '030714']
    distrito04=['040101', '040102', '040103', '040104', '040105', '040106', '040107', '040108', '040109', '040110', '040111', '040112', '040113', '040114', '040115', '040116', '040117', '040118', '040119', '040120', '040121', '040122', '040123', '040124', '040125', '040126', '040127', '040128', '040129', '040201', '040202', '040203', '040204', '040205', '040206', '040207', '040208', '040301', '040302', '040303', '040304', '040305', '040306', '040307', '040308', '040309', '040310', '040311', '040312', '040313', '040401', '040402', '040403', '040404', '040405', '040406', '040407', '040408', '040409', '040410', '040411', '040412', '040413', '040414', '040501', '040502', '040503', '040504', '040505', '040506', '040507', '040508', '040509', '040510', '040511', '040512', '040513', '040514', '040515', '040516', '040517', '040518', '040519', '040520', '040601', '040602', '040603', '040604', '040605', '040606', '040607', '040608', '040701', '040702', '040703', '040704', '040705', '040706', '040801', '040802', '040803', '040804', '040805', '040806', '040807', '040808', '040809', '040810', '040811']
    distrito05=['050101', '050102', '050103', '050104', '050105', '050106', '050107', '050108', '050109', '050110', '050111', '050112', '050113', '050114', '050115', '050116', '050201', '050202', '050203', '050204', '050205', '050206', '050301', '050302', '050303', '050304', '050401', '050402', '050403', '050404', '050405', '050406', '050407', '050408', '050409', '050410', '050411', '050412', '050413', '050501', '050502', '050503', '050504', '050505', '050506', '050507', '050508', '050509', '050510', '050511', '050512', '050513', '050514', '050515', '050601', '050602', '050603', '050604', '050605', '050606', '050607', '050608', '050609', '050610', '050611', '050612', '050613', '050614', '050615', '050616', '050617', '050618', '050619', '050620', '050621', '050701', '050702', '050703', '050704', '050705', '050706', '050707', '050708', '050801', '050802', '050803', '050804', '050805', '050806', '050807', '050808', '050809', '050810', '050901', '050902', '050903', '050904', '050905', '050906', '050907', '050908', '050909', '050910', '050911', '051001', '051002', '051003', '051004', '051005', '051006', '051007', '051008', '051009', '051010', '051011', '051012', '051101', '051102', '051103', '051104', '051105', '051106', '051107', '051108']
    distrito06=['060101', '060102', '060103', '060104', '060105', '060106', '060107', '060108', '060109', '060110', '060111', '060112', '060201', '060202', '060203', '060204', '060301', '060302', '060303', '060304', '060305', '060306', '060307', '060308', '060309', '060310', '060311', '060312', '060401', '060402', '060403', '060404', '060405', '060406', '060407', '060408', '060409', '060410', '060411', '060412', '060413', '060414', '060415', '060416', '060417', '060418', '060419', '060501', '060502', '060503', '060504', '060505', '060506', '060507', '060508', '060601', '060602', '060603', '060604', '060605', '060606', '060607', '060608', '060609', '060610', '060611', '060612', '060613', '060614', '060615', '060701', '060702', '060703', '060801', '060802', '060803', '060804', '060805', '060806', '060807', '060808', '060809', '060810', '060811', '060812', '060901', '060902', '060903', '060904', '060905', '060906', '060907', '061001', '061002', '061003', '061004', '061005', '061006', '061007', '061101', '061102', '061103', '061104', '061105', '061106', '061107', '061108', '061109', '061110', '061111', '061112', '061113', '061201', '061202', '061203', '061204', '061301', '061302', '061303', '061304', '061305', '061306', '061307', '061308', '061309', '061310', '061311']
    distrito07=['070101', '070102', '070103', '070104', '070105', '070106', '070107']
    distrito08=['080101', '080102', '080103', '080104', '080105', '080106', '080107', '080108', '080201', '080202', '080203', '080204', '080205', '080206', '080207', '080301', '080302', '080303', '080304', '080305', '080306', '080307', '080308', '080309', '080401', '080402', '080403', '080404', '080405', '080406', '080407', '080408', '080501', '080502', '080503', '080504', '080505', '080506', '080507', '080508', '080601', '080602', '080603', '080604', '080605', '080606', '080607', '080608', '080701', '080702', '080703', '080704', '080705', '080706', '080707', '080708', '080801', '080802', '080803', '080804', '080805', '080806', '080807', '080808', '080901', '080902', '080903', '080904', '080905', '080906', '080907', '080908', '080909', '080910', '080911', '080912', '080913', '080914', '080915', '080916', '080917', '080918', '081001', '081002', '081003', '081004', '081005', '081006', '081007', '081008', '081009', '081101', '081102', '081103', '081104', '081105', '081106', '081201', '081202', '081203', '081204', '081205', '081206', '081207', '081208', '081209', '081210', '081211', '081212', '081301', '081302', '081303', '081304', '081305', '081306', '081307']
    distrito09=['090101', '090102', '090103', '090104', '090105', '090106', '090107', '090108', '090109', '090110', '090111', '090112', '090113', '090114', '090115', '090116', '090117', '090118', '090119', '090201', '090202', '090203', '090204', '090205', '090206', '090207', '090208', '090301', '090302', '090303', '090304', '090305', '090306', '090307', '090308', '090309', '090310', '090311', '090312', '090401', '090402', '090403', '090404', '090405', '090406', '090407', '090408', '090409', '090410', '090411', '090412', '090413', '090501', '090502', '090503', '090504', '090505', '090506', '090507', '090508', '090509', '090510', '090511', '090601', '090602', '090603', '090604', '090605', '090606', '090607', '090608', '090609', '090610', '090611', '090612', '090613', '090614', '090615', '090616', '090701', '090702', '090703', '090704', '090705', '090706', '090707', '090709', '090710', '090711', '090713', '090714', '090715', '090716', '090717', '090718', '090719', '090720', '090721', '090722', '090723', '090724', '090725']
    distrito10=['100101', '100102', '100103', '100104', '100105', '100106', '100107', '100108', '100109', '100110', '100111', '100112', '100113', '100201', '100202', '100203', '100204', '100205', '100206', '100207', '100208', '100301', '100307', '100311', '100313', '100316', '100317', '100321', '100322', '100323', '100401', '100402', '100403', '100404', '100501', '100502', '100503', '100504', '100505', '100506', '100507', '100508', '100509', '100510', '100511', '100601', '100602', '100603', '100604', '100605', '100606', '100607', '100608', '100609', '100610', '100701', '100702', '100703', '100704', '100705', '100801', '100802', '100803', '100804', '100901', '100902', '100903', '100904', '100905', '101001', '101002', '101003', '101004', '101005', '101006', '101007', '101101', '101102', '101103', '101104', '101105', '101106', '101107', '101108']
    distrito11=['110101', '110102', '110103', '110104', '110105', '110106', '110107', '110108', '110109', '110110', '110111', '110112', '110113', '110114', '110201', '110202', '110203', '110204', '110205', '110206', '110207', '110208', '110209', '110210', '110211', '110301', '110302', '110303', '110304', '110305', '110401', '110402', '110403', '110404', '110405', '110501', '110502', '110503', '110504', '110505', '110506', '110507', '110508']
    distrito12=['120101', '120104', '120105', '120106', '120107', '120108', '120111', '120112', '120113', '120114', '120116', '120117', '120119', '120120', '120121', '120122', '120124', '120125', '120126', '120127', '120128', '120129', '120130', '120132', '120133', '120134', '120135', '120136', '120201', '120202', '120203', '120204', '120205', '120206', '120207', '120208', '120209', '120210', '120211', '120212', '120213', '120214', '120215', '120301', '120302', '120303', '120304', '120305', '120306', '120401', '120402', '120403', '120404', '120405', '120406', '120407', '120408', '120409', '120410', '120411', '120412', '120413', '120414', '120415', '120416', '120417', '120418', '120419', '120420', '120421', '120422', '120423', '120424', '120425', '120426', '120427', '120428', '120429', '120430', '120431', '120432', '120433', '120434', '120501', '120502', '120503', '120504', '120601', '120602', '120603', '120604', '120605', '120606', '120607', '120608', '120609', '120701', '120702', '120703', '120704', '120705', '120706', '120707', '120708', '120709', '120801', '120802', '120803', '120804', '120805', '120806', '120807', '120808', '120809', '120810', '120901', '120902', '120903', '120904', '120905', '120906', '120907', '120908', '120909', '130101']
    distrito13=['130102', '130103', '130104', '130105', '130106', '130107', '130108', '130109', '130110', '130111', '130112', '130201', '130202', '130203', '130204', '130205', '130206', '130207', '130208', '130301', '130302', '130303', '130304', '130305', '130306', '130401', '130402', '130403', '130501', '130502', '130503', '130504', '130601', '130602', '130604', '130605', '130606', '130608', '130610', '130611', '130613', '130614', '130701', '130702', '130703', '130704', '130705', '130801', '130802', '130803', '130804', '130805', '130806', '130807', '130808', '130809', '130810', '130811', '130812', '130813', '130901', '130902', '130903', '130904', '130905', '130906', '130907', '130908', '131001', '131002', '131003', '131004', '131005', '131006', '131007', '131008', '131101', '131102', '131103', '131104', '131201', '131202', '131203']
    distrito14=['140101', '140102', '140103', '140104', '140105', '140106', '140107', '140108', '140109', '140110', '140111', '140112', '140113', '140114', '140115', '140116', '140117', '140118', '140119', '140120', '140201', '140202', '140203', '140204', '140205', '140206', '140301', '140302', '140303', '140304', '140305', '140306', '140307', '140308', '140309', '140310', '140311', '140312']
    distrito15=['150101', '150102', '150103', '150104', '150105', '150106', '150107', '150108', '150109', '150110', '150111', '150112', '150113', '150114', '150115', '150116', '150117', '150118', '150119', '150120', '150121', '150122', '150123', '150124', '150125', '150126', '150127', '150128', '150129', '150130', '150131', '150132', '150133', '150134', '150135', '150136', '150137', '150138', '150139', '150140', '150141', '150142', '150143', '150201', '150202', '150203', '150204', '150205', '150301', '150302', '150303', '150304', '150305', '150401', '150402', '150403', '150404', '150405', '150406', '150407', '150501', '150502', '150503', '150504', '150505', '150506', '150507', '150508', '150509', '150510', '150511', '150512', '150513', '150514', '150515', '150516', '150601', '150602', '150603', '150604', '150605', '150606', '150607', '150608', '150609', '150610', '150611', '150612', '150701', '150702', '150703', '150704', '150705', '150706', '150707', '150708', '150709', '150710', '150711', '150712', '150713', '150714', '150715', '150716', '150717', '150718', '150719', '150720', '150721', '150722', '150723', '150724', '150725', '150726', '150727', '150728', '150729', '150730', '150731', '150732', '150801', '150802', '150803', '150804', '150805', '150806', '150807', '150808', '150809', '150810', '150811', '150812', '150901', '150902', '150903', '150904', '150905', '150906', '151001', '151002', '151003', '151004', '151005', '151006', '151007', '151008', '151009', '151010', '151011', '151012', '151013', '151014', '151015', '151016', '151017', '151018', '151019', '151020', '151021', '151022', '151023', '151024', '151025', '151026', '151027', '151028', '151029', '151030', '151031', '151032', '151033']
    distrito16=['160101', '160102', '160103', '160104', '160105', '160106', '160107', '160108', '160110', '160112', '160113', '160201', '160202', '160205', '160206', '160210', '160211', '160301', '160302', '160303', '160304', '160305', '160401', '160402', '160403', '160404', '160501', '160502', '160503', '160504', '160505', '160506', '160507', '160508', '160509', '160510', '160511', '160601', '160602', '160603', '160604', '160605', '160606', '160701', '160702', '160703', '160704', '160705', '160706', '160801', '160802', '160803', '160804']
    distrito17=['170101', '170102', '170103', '170104', '170201', '170202', '170203', '170204', '170301', '170302', '170303']
    distrito18=['180101', '180102', '180103', '180104', '180105', '180106', '180107', '180201', '180202', '180203', '180204', '180205', '180206', '180207', '180208', '180209', '180210', '180211', '180301', '180302', '180303']
    distrito19=['190101', '190102', '190103', '190104', '190105', '190106', '190107', '190108', '190109', '190110', '190111', '190112', '190113', '190201', '190202', '190203', '190204', '190205', '190206', '190207', '190208', '190301', '190302', '190303', '190304', '190305', '190306', '190307', '190308']
    distrito20=['200101', '200104', '200105', '200107', '200108', '200109', '200110', '200111', '200114', '200115', '200201', '200202', '200203', '200204', '200205', '200206', '200207', '200208', '200209', '200210', '200301', '200302', '200303', '200304', '200305', '200306', '200307', '200308', '200401', '200402', '200403', '200404', '200405', '200406', '200407', '200408', '200409', '200410', '200501', '200502', '200503', '200504', '200505', '200506', '200507', '200601', '200602', '200603', '200604', '200605', '200606', '200607', '200608', '200701', '200702', '200703', '200704', '200705', '200706', '200801', '200802', '200803', '200804', '200805', '200806']
    distrito21=['210101', '210102', '210103', '210104', '210105', '210106', '210107', '210108', '210109', '210110', '210111', '210112', '210113', '210114', '210115', '210201', '210202', '210203', '210204', '210205', '210206', '210207', '210208', '210209', '210210', '210211', '210212', '210213', '210214', '210215', '210301', '210302', '210303', '210304', '210305', '210306', '210307', '210308', '210309', '210310', '210401', '210402', '210403', '210404', '210405', '210406', '210407', '210501', '210502', '210503', '210504', '210505', '210601', '210602', '210603', '210604', '210605', '210606', '210607', '210608', '210701', '210702', '210703', '210704', '210705', '210706', '210707', '210708', '210709', '210710', '210801', '210802', '210803', '210804', '210805', '210806', '210807', '210808', '210809', '210901', '210902', '210903', '210904', '211001', '211002', '211003', '211004', '211005', '211101', '211102', '211103', '211104', '211105', '211201', '211202', '211203', '211204', '211205', '211206', '211207', '211208', '211209', '211210', '211301', '211302', '211303', '211304', '211305', '211306', '211307']
    distrito22=['220101', '220102', '220103', '220104', '220105', '220106', '220201', '220202', '220203', '220204', '220205', '220206', '220301', '220302', '220303', '220304', '220305', '220401', '220402', '220403', '220404', '220405', '220406', '220501', '220502', '220503', '220504', '220505', '220506', '220507', '220508', '220509', '220510', '220511', '220601', '220602', '220603', '220604', '220605', '220701', '220702', '220703', '220704', '220705', '220706', '220707', '220708', '220709', '220710', '220801', '220802', '220803', '220804', '220805', '220806', '220807', '220808', '220809', '220901', '220902', '220903', '220904', '220905', '220906', '220907', '220908', '220909', '220910', '220911', '220912', '220913', '220914', '221001', '221002', '221003', '221004', '221005', '221006']
    distrito23=['230101', '230102', '230103', '230104', '230105', '230106', '230107', '230108', '230109', '230110', '230111', '230201', '230202', '230203', '230204', '230205', '230206', '230301', '230302', '230303', '230401', '230402', '230403', '230404', '230405', '230406', '230407', '230408']
    distrito24=['240101', '240102', '240103', '240104', '240105', '240106', '240201', '240202', '240203', '240301', '240302', '240303', '240304']
    distrito25=['250101', '250102', '250103', '250104', '250105', '250106', '250107', '250201', '250202', '250203', '250204', '250301', '250302', '250303', '250304', '250305', '250306', '250307', '250401']
    
    print(f'Iniciando {obtenerNombreVariable(distritos010203)} | {FechaHoraActual()}')
    main(distrito06)   
    #procesarURL('030217', '30=', 'Proyecto', '2021', '300276', conexion, True)
    