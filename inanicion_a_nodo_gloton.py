"""
Experimento A — Nodo glotón (Anillo de Token).

El nodo 0 quiere la SC siempre, se demora más dentro de ella y casi no espera
fuera. Se observa si los demás nodos dejan de entrar (inanición).
"""
from multiprocessing import Process, Queue, Array
import sys
import time
import random

NUM_NODOS = 3
NODO_GLOTON = 0
RONDAS = 5  # EXPERIMENTO A: más rondas (antes 2) para que el efecto se note


def log(msg):
    ahora = time.time()
    milisegundos = int((ahora % 1) * 1000)
    marca = time.strftime("%H:%M:%S", time.localtime(ahora)) + f".{milisegundos:03d}"
    # la línea completa se escribe de una sola vez para que dos procesos
    # no mezclen sus mensajes en la misma línea
    sys.stdout.write(f"{marca} {msg}\n")
    sys.stdout.flush()


def execute_critical_section(node_id, algo_name):
    log(f"[{algo_name}] NODO {node_id} ENTRANDO a la Sección Crítica...")
    if node_id == NODO_GLOTON:
        time.sleep(3)  # EXPERIMENTO A: el nodo glotón se demora más dentro de la SC
    else:
        time.sleep(random.uniform(0.5, 1.5))  # Simula trabajo en SC
    log(f"[{algo_name}] NODO {node_id} SALIENDO de la Sección Crítica.")


def token_ring_node(node_id, queues, num_nodes, entradas, initial_token=False):
    has_token = initial_token
    if has_token:
        log(f"[Anillo] Nodo {node_id} inicia con el Token")

    for _ in range(RONDAS):
        if not has_token:
            # Esperar Token
            msg = queues[node_id].get()
            if msg == "TOKEN":
                has_token = True
                log(f"[Anillo] Nodo {node_id} recibe Token")

        # Decidir si queremos la SC
        if node_id == NODO_GLOTON:
            want_cs = True  # EXPERIMENTO A: el nodo glotón SIEMPRE quiere la SC
        else:
            want_cs = random.choice([True, True, False])  # Probabilidad alta de quererla

        if want_cs:
            execute_critical_section(node_id, "Anillo-A")
            with entradas.get_lock():
                entradas[node_id] += 1  # se cuenta la entrada para el resumen final
        else:
            log(f"[Anillo] Nodo {node_id} no quiere la SC (want_cs = False)")

        # Pasar Token (obligatorio después de cada uso)
        has_token = False
        next_node = (node_id + 1) % num_nodes
        # se registra antes del put() para que salga antes que el "recibe Token"
        log(f"[Anillo] Nodo {node_id} pasa Token a Nodo {next_node}")
        queues[next_node].put("TOKEN")

        if node_id == NODO_GLOTON:
            time.sleep(0.1)  # EXPERIMENTO A: el nodo glotón casi no espera fuera de la SC
        else:
            time.sleep(random.uniform(0.5, 1.5))  # Simula trabajo fuera de SC


def run_experimento_a():
    print("--- Experimento A: Nodo glotón en el Anillo de Token ---", flush=True)
    queues = [Queue() for _ in range(NUM_NODOS)]
    entradas = Array('i', NUM_NODOS)  # contador compartido de entradas por nodo

    nodes = [Process(target=token_ring_node, args=(i, queues, NUM_NODOS, entradas, i == 0))
             for i in range(NUM_NODOS)]
    inicio = time.time()
    for n in nodes: n.start()
    for n in nodes: n.join()
    duracion = time.time() - inicio

    # EXPERIMENTO A: resumen de cuántas veces entró cada nodo a la SC
    print("\n========== RESUMEN EXPERIMENTO A ==========", flush=True)
    for i in range(NUM_NODOS):
        etiqueta = " (glotón)" if i == NODO_GLOTON else ""
        print(f"Nodo {i}{etiqueta}: entró {entradas[i]} veces a la SC (de {RONDAS} rondas)", flush=True)
    print(f"Duración total: {duracion:.1f} s", flush=True)
    otros = [i for i in range(NUM_NODOS) if i != NODO_GLOTON]
    if all(entradas[i] > 0 for i in otros):
        print("Conclusión: NO hay inanición; los demás nodos siguen entrando porque "
              "el token se pasa siempre después de cada uso.", flush=True)
    else:
        print("Conclusión: algún nodo no entró a la SC; revisar la salida.", flush=True)


if __name__ == '__main__':
    run_experimento_a()
