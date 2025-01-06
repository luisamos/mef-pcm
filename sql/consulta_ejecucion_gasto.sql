CREATE SCHEMA mef
    AUTHORIZATION postgres;

CREATE TABLE IF NOT EXISTS mef.consulta_ejecucion_gasto
(
	id serial,
	nivel_gobierno character varying(1), -- R: Regional, M: Municipalidad provincial o distrital
	ubigeo character varying(6),
	tipo_gasto character varying(1), --Categoria presupuestal: c, Producto/Proyecto: p, funcion: f
	acti_proy character varying(1), -- Actividad/ Proyecto: 1, Actividades: 3, Proyectos: 2
	anio smallint,
	codigo character varying(10),
	descri text,
	pia bigint,
	pim bigint,
	certi bigint,
	comp_anua bigint,
	eje_aten_comp_men bigint,
	devengado bigint,
	girado bigint,
	avance numeric(4,1)	
);

--DROP FUNCTION IF EXISTS mef.get_consulta_ejecucion_gasto;
CREATE OR REPLACE FUNCTION mef.get_consulta_ejecucion_gasto(codigo_ubigeo character varying, cp character varying)
RETURNS TABLE (
    ano_eje smallint,
    tipo_gobierno character varying(1),
    code character varying(10),
    descr text,
    tip_act_proy integer,
    pim bigint,
    dev bigint,
    gir bigint
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        a.anio AS ano_eje, 
        a.nivel_gobierno AS tipo_gobierno,
        a.codigo AS code, 
        a.descri AS descr, 
        a.acti_proy :: integer AS tip_act_proy,
        a.pim,
        a.devengado AS dev,
        a.girado as gir
    FROM mef.consulta_ejecucion_gasto a
    WHERE a.anio BETWEEN 2016 AND 2023
      AND a.ubigeo = codigo_ubigeo
	  AND a.tipo_gasto= cp
    ORDER BY a.anio DESC;
END;
$$ LANGUAGE plpgsql;