create table censados (
provincia_id int not null,
funcion varchar(200),
nombre varchar (200),
categoria varchar (200),
coord_lat float,
coord_long float,
municipio_id int not null,
departamento_id int not null,
id int not null primary key,
fuente varchar (200)
);

create table provincia (
nombre varchar (200),
id int not null primary key
);

create table municipio (
nombre varchar (200),
id int not null primary key
);

create table departamento (
nombre varchar (200),
id int not null primary key
);

alter table censados 
add foreign key (provincia_id) references provincia (id);

alter table censados 
add foreign key (municipio_id) references municipio (id);

alter table censados 
add foreign key (departamento_id) references departamento (id);