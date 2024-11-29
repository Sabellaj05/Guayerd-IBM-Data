USE db_fundacion_final;

SELECT * FROM tipo_contribuyentes;

SELECT * FROM categoria_proveedores;

SELECT * FROM razones_sociales;

SELECT * FROM ciudades;

SELECT * FROM cuentas
ORDER BY id_cuenta;


SELECT * FROM gastos;

SELECT * FROM proveedores;

SELECT * FROM donantes;

SELECT * FROM ingresos;

SELECT * FROM tipo_donantes;

SELECT * FROM paises;

SELECT * FROM frecuencias;



SELECT COUNT(p.nombre) AS cant_personas, c.ciudad
FROM proveedores AS p
JOIN ciudades AS c
ON
p.id_ciudad = c.id_ciudad
#WHERE c.ciudad IN ("Buenos Aires", "Rosario", "Cordoba", "Mendoza")
GROUP BY c.ciudad
#HAVING COUNT(p.nombre) > 5
ORDER BY cant_personas DESC;



-- donantes
USE db_fundacion_dev;
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

# Suma de ingresos totales de los diferentes tipos de donantes con frecuencia mensual
SELECT t.tipo, SUM(ing.importe) AS importe_total, f.frecuencia
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
WHERE f.frecuencia = "Mensual"
GROUP BY t.tipo_donantes
HAVING importe_total > 10000000
ORDER BY importe_total DESC;
