<h1 align="center"> Analisis de Tweets Secuencial </h1>

## Comenzando 🚀

_Estas instrucciones te permitirán obtener una copia del proyecto en funcionamiento en tu máquina local para propósitos de desarrollo y pruebas._

Mira **Deployment** para conocer como desplegar el proyecto.

### Pre-requisitos 📋

_Se necesita tener Docker instalado en tu equipo local_

## Despliegue 📦

_Primero necesitas construir la imagen a usar_
docker build --tag pythoni .

_Para ejecutar_

* Opción 1:
    _Ejecutas para estar en la terminal del contenedor y desde allí ejecutar el programa_

    docker run  --name pyproject -it -d -v **la ruta donde guardes este repositorio**:/app pythoni /bin/bash

    docker exec tweet5 python3 /app/generador.py

* Opción 2:
    _Se ejecuta directamente el programa_

    docker run --rm -v **la ruta donde guardes este repositorio**:/app pythoni python3 /app/generador.py


## Construido con 🛠️

_Menciona las herramientas que utilizaste para crear tu proyecto_

* [Visual Studio Code](https://code.visualstudio.com/) - Para el desarrollo
* [Docker](https://www.docker.com/) - Para el contenedor

## Autores ✒️

_Menciona a todos aquellos que ayudaron a levantar el proyecto desde sus inicios_

* **Tabata Llach** - *Desarrollo* - [tllach](https://github.com/tllach)
* **Katy Diaz** - *Desarrollo* - [Katy Diaz](https://github.com/Katkat04)
* **Juan Anzola** - *Desarrollo* - [JuanDa-dev](https://github.com/JuanDa-dev)
