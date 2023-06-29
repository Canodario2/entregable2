create table censados (
provincia_id int not null,
funcion varchar(20),
nombre varchar (50),
categoria varchar (50),
coord_lat float,
coord_long float,
municipio_id int not null,
departamento_id int not null,
id int not null primary key,
fuente varchar (30)
);

create table provincia (
nombre varchar (50),
id int not null primary key
);

create table municipio (
nombre varchar (50),
id int not null primary key
);

create table departamento (
nombre varchar (50),
id int not null primary key
);

alter table censados 
add foreign key (provincia_id) references provincia (id);

alter table censados 
add foreign key (municipio_id) references municipio (id);

alter table censados 
add foreign key (departamento_id) references departamento (id);