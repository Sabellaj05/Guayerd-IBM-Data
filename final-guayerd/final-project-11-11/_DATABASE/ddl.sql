-- Create the tables without relationships -- 
DROP DATABASE IF EXISTS db_fundacion_final;

CREATE DATABASE IF NOT EXISTS db_fundacion_final;
USE db_fundacion_final;

CREATE TABLE IF NOT EXISTS proveedores (
  id INT PRIMARY KEY AUTO_INCREMENT,
  numero VARCHAR(100) NOT NULL UNIQUE,
  nombre VARCHAR(100) NOT NULL,
  cuit VARCHAR(100) NOT NULL, -- deberia ser UNIQUE tambien en teoria
  contacto VARCHAR(100),
  mail VARCHAR(255),   -- tuve que sacar el UNIQUE porque hay duplicados
	telefono VARCHAR(30) 
);

CREATE TABLE IF NOT EXISTS donantes (
  id INT PRIMARY KEY AUTO_INCREMENT,
  numero VARCHAR(100) NOT NULL UNIQUE,
  nombre VARCHAR(100) NOT NULL,
  cuit VARCHAR(100) NOT NULL UNIQUE,
  contacto VARCHAR(100),
  mail VARCHAR(255) UNIQUE,   
	telefono VARCHAR(50),
  activo BOOL NOT NULL 
  ## activo TINYINT(1) NOT NULL UNIQUE  ## -> El problema era que puse UNIQUE sin querer
);


CREATE TABLE IF NOT EXISTS categoria_proveedores (
  id_categoria INT PRIMARY KEY AUTO_INCREMENT,
  categoria VARCHAR(100) NOT NULL UNIQUE 
);

CREATE TABLE IF NOT EXISTS tipo_contribuyentes (
  id_contribuyente INT PRIMARY KEY AUTO_INCREMENT,
 	contribuyente VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS razones_sociales (
  id_razon INT PRIMARY KEY AUTO_INCREMENT,
  razon VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS ciudades (
  id_ciudad INT PRIMARY KEY AUTO_INCREMENT,
  ciudad VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS paises (
  id_pais INT PRIMARY KEY AUTO_INCREMENT,
  pais VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS tipo_donantes (
  id_tipo INT PRIMARY KEY AUTO_INCREMENT,
  tipo VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS ingresos (
  id INT PRIMARY KEY AUTO_INCREMENT,
  importe DECIMAL(10, 2) NOT NULL,
  fecha DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS frecuencias (
  id_frecuencia INT PRIMARY KEY AUTO_INCREMENT,
  frecuencia VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS gastos (
  id INT PRIMARY KEY AUTO_INCREMENT,
  importe DECIMAL(10, 2) NOT NULL,
  fecha DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS cuentas (
  id_cuenta INT PRIMARY KEY AUTO_INCREMENT,
  nro_cuenta INT NOT NULL UNIQUE	 
);

### --->  Altering tables for relationships <--- ###

## Proveedores y Donantes

ALTER TABLE proveedores 
  ADD COLUMN id_categoria INT NOT NULL,
  ADD COLUMN id_contribuyente INT NOT NULL,
  ADD COLUMN id_razon INT NOT NULL,
  ADD COLUMN id_ciudad INT NOT NULL;
  
ALTER TABLE proveedores 
  ADD CONSTRAINT fk_proveedor_categoria FOREIGN KEY (id_categoria) REFERENCES categoria_proveedores(id_categoria),
  ADD CONSTRAINT fk_proveedor_contribuyente FOREIGN KEY (id_contribuyente) REFERENCES tipo_contribuyentes(id_contribuyente),
  ADD CONSTRAINT fk_proveedor_razon FOREIGN KEY (id_razon) REFERENCES razones_sociales(id_razon),
  ADD CONSTRAINT fk_proveedor_ciudad FOREIGN KEY (id_ciudad) REFERENCES ciudades(id_ciudad);
  

ALTER TABLE donantes 
  ADD COLUMN id_frecuencia INT NOT NULL,
  ADD COLUMN id_contribuyente INT NOT NULL,
  ADD COLUMN id_tipo INT NOT NULL,
  ADD COLUMN id_razon INT NOT NULL,
  ADD COLUMN id_pais INT NOT NULL;

ALTER TABLE donantes
	ADD CONSTRAINT fk_donante_frecuencia FOREIGN KEY (id_frecuencia) REFERENCES frecuencias(id_frecuencia),
	ADD CONSTRAINT fk_donante_contribuyente FOREIGN KEY (id_contribuyente) REFERENCES tipo_contribuyentes(id_contribuyente),
	ADD CONSTRAINT fk_donante_tipo FOREIGN KEY (id_tipo) REFERENCES tipo_donantes(id_tipo),
	ADD CONSTRAINT fk_donante_razon FOREIGN KEY (id_razon) REFERENCES razones_sociales(id_razon),
	ADD CONSTRAINT fk_donante_pais FOREIGN KEY (id_pais) REFERENCES paises(id_pais);
  
	
## Transacciones

-- columns
ALTER TABLE gastos
  ADD COLUMN id_proveedor INT NOT NULL,
  ADD COLUMN id_cuenta INT NOT NULL;

-- foreign keys
ALTER TABLE gastos 
 	#ADD CONSTRAINT fk_transacciones_proveedor FOREIGN KEY (id) REFERENCES proveedores(id),
 	ADD CONSTRAINT fk_gastos_proveedor FOREIGN KEY (id_proveedor) REFERENCES proveedores(id),
 	ADD CONSTRAINT fk_gastos_cuenta FOREIGN KEY (id_cuenta) REFERENCES cuentas(id_cuenta);

ALTER TABLE ingresos
  ADD COLUMN id_donante INT NOT NULL,
  ADD COLUMN id_cuenta INT NOT NULL;

-- foreign keys
ALTER TABLE ingresos
 	ADD CONSTRAINT fk_ingresos_donante FOREIGN KEY (id_donante) REFERENCES donantes(id),
 	ADD CONSTRAINT fk_ingresos_cuenta FOREIGN KEY (id_cuenta) REFERENCES cuentas(id_cuenta);
