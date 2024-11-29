USE db_fundacion_final;

-- No funca
-- SET @cities = ("Buenos Aires", "Rosario", "Cordoba", "Mendoza");

SELECT COUNT(p.nombre) AS cant_personas, c.ciudad
FROM proveedores AS p
JOIN ciudades AS c
ON
p.id_ciudad = c.id_ciudad
WHERE c.ciudad IN ("Buenos Aires", "Rosario", "Cordoba", "Mendoza")
GROUP BY c.ciudad
-- HAVING COUNT(p.nombre) > 5
ORDER BY cant_personas DESC;



-- razon social por donantes
SELECT COUNT(d.numero) AS cant_donantes, r.razon
FROM donantes AS d
JOIN razones_sociales AS r
ON
d.id_razon = r.id_razon
GROUP BY r.razon
ORDER BY cant_donantes DESC;



-- donantes
# ingresos totales de las diferentes ONG
SELECT d.id, d.numero, d.nombre, SUM(ing.importe) AS importe_total, t.tipo
FROM donantes AS d
JOIN tipo_donantes AS t
ON 
d.id_tipo = t.id_tipo
JOIN ingresos AS ing
ON d.id = ing.id_donante
WHERE tipo = "ONG"
GROUP BY nombre, d.id, d.numero;




-- Suma de ingresos totales de los diferentes tipos de donantes con frecuencia mensual
SELECT t.tipo AS tipo_donante, SUM(ing.importe) AS importe_total, f.frecuencia
FROM donantes as d
JOIN frecuencias AS f
ON
d.id_frecuencia = f.id_frecuencia
JOIN ingresos AS ing
ON
d.id = ing.id_donante
JOIN tipo_donantes AS t
ON
d.id_tipo = t.id_tipo
WHERE f.frecuencia = "mensual"
GROUP BY t.tipo
HAVING importe_total > 10000000
ORDER BY importe_total DESC;


-- Mayores donaciones por tipo de donante, por anio
SELECT 
    r.razon AS `razon_social`,
    COUNT(DISTINCT d.numero) AS `cant_donantes`,
    COUNT(i.importe) AS `cant_donaciones`,
    SUM(i.importe) AS `importe_total`,
    YEAR(i.fecha) AS `anio`
FROM donantes d
JOIN razones_sociales r
ON
d.id_razon = r.id_razon
JOIN ingresos i
ON
i.id_donante = d.id
GROUP BY r.razon, YEAR(i.fecha)
HAVING importe_total > 10000000
ORDER BY importe_total DESC, YEAR(i.fecha) ASC;

-- Ver quien es ese unico donante que hizo el mayor aporte en 2019
SELECT d.nombre, i.fecha, SUM(i.importe) AS `total_importe`, r.razon
FROM donantes d
JOIN ingresos i
ON
d.id = i.id_donante
JOIN razones_sociales r
ON
d.id_razon = r.id_razon
WHERE r.razon = "S.A.S"
AND
YEAR(i.fecha) = 2019
GROUP BY d.nombre, i.fecha;
