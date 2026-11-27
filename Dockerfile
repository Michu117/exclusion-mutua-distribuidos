# Imagen para la APE 9: Algoritmos de exclusión mutua distribuida
# Python 3.11 cumple el requisito "Python 3.8 o superior"
FROM python:3.11-slim

LABEL asignatura="Sistemas Distribuidos" \
      practica="APE 9 - Implementación de algoritmos de exclusión mutua" \
      autor="Fabricio Josue Ruiz Aguilar"

# PYTHONUNBUFFERED=1 es obligatorio: sin esto los print de los procesos salen
# tarde o desordenados y no se ve la concurrencia
ENV PYTHONUNBUFFERED=1 \
    TZ=America/Guayaquil

WORKDIR /app

# Solo librerías estándar: no hace falta pip install
COPY *.py ./

# La opción del menú se pasa como argumento: docker run ... 2
ENTRYPOINT ["python", "algoritmos_exclusion.py"]
