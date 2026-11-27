# exclusion-mutua-distribuidos — APE 9: Implementación de algoritmos de exclusión mutua

**Asignatura:** Sistemas Distribuidos — 6to ciclo, Unidad 2
**Estudiante:** Fabricio Josue Ruiz Aguilar

Simulación de tres algoritmos de exclusión mutua distribuida con `multiprocessing`
(solo librerías estándar de Python) y un experimento de inanición sobre el Anillo de Token.
Todo se ejecuta **dentro de un contenedor Docker**.

## Archivos

| Archivo | Contenido |
|---|---|
| `algoritmos_exclusion.py` | Código base de la guía corregido: Centralizado, Ricart–Agrawala y Anillo de Token |
| `inanicion_a_nodo_gloton.py` | Experimento A: el nodo 0 siempre quiere la SC y se demora más |
| `inanicion_b_retiene_token.py` | Experimento B: el nodo 0 entra varias veces seguidas sin soltar el token |
| `algoritmos_original.py` | Versión original sin corregir (solo para comparar, captura opcional 06) |
| `Dockerfile`, `docker-compose.yml`, `.dockerignore` | Dockerización |
| `capturas/` | Capturas de pantalla de la práctica |
| `salidas/` | Salidas de consola guardadas en `.txt` |

## Requisitos

- Docker y Docker Compose v2 (`docker --version`, `docker compose version`).

## Construir la imagen

```bash
docker compose build
docker images | grep ape9
```

La imagen se llama `ape9-exclusion-mutua`.

## Ejecutar

Ningún algoritmo arranca con `docker compose up` (solo muestra una ayuda).
Cada ejecución se lanza por separado:

```bash
docker compose run --rm centralizado    | tee salidas/01_centralizado.txt
docker compose run --rm ricart-agrawala | tee salidas/02_ricart_agrawala.txt
docker compose run --rm anillo          | tee salidas/03_anillo_token.txt
docker compose run --rm inanicion-a     | tee salidas/04_inanicion_A.txt
docker compose run --rm inanicion-b     | tee salidas/05_inanicion_B.txt
```

Otras formas de elegir el algoritmo:

```bash
docker run -it --rm ape9-exclusion-mutua          # menú interactivo (escribir 1, 2 o 3)
docker run --rm ape9-exclusion-mutua 2            # opción por argumento
docker run --rm -e ALGORITMO=3 ape9-exclusion-mutua   # opción por variable de entorno
```

Versión original sin corregir (para comparar):

```bash
docker run --rm -v "$PWD":/app --entrypoint python ape9-exclusion-mutua algoritmos_original.py 2
```

## Recursos

Cada servicio tiene `cpus: "4"` y `mem_limit: 4g`, que son los recursos que pide la guía.

## Correcciones al código original

Cada cambio está marcado en el código con `# CORRECCIÓN:`.

- **Marca de tiempo** `HH:MM:SS.mmm` en cada mensaje (función `log`), escrita en una sola
  operación para que los procesos no mezclen líneas.
- **Servidor centralizado:** `time.sleep(0.01)` en el bucle para evitar la espera activa.
- **Ricart–Agrawala:** hilo receptor por nodo que atiende los mensajes todo el tiempo
  (RELEASED → responde, HELD → difiere, WANTED → compara `(tiempo, id)`), mensajes
  `DONE` para que cada nodo siga respondiendo hasta que todos terminen, y logs de
  REPLY enviados y diferidos.
- **Anillo de Token:** logs de cuándo un nodo recibe el token y cuándo no quiere la SC.

## Nota técnica

Los "nodos" son procesos (`multiprocessing`) **dentro de un mismo contenedor** y se
comunican por `multiprocessing.Queue`. Docker se usa para que el entorno sea
reproducible; no hay un contenedor por nodo. Para eso habría que cambiar las colas
por sockets o un broker de mensajes.
