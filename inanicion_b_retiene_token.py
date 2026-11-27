"""
Experimento B — Nodo que retiene el token (Anillo de Token).

Igual que el experimento A, pero el nodo 0 entra VARIAS VECES SEGUIDAS a la SC
antes de pasar el token. Mientras no lo suelta, los demás no pueden entrar.
"""
from multiprocessing import Process, Queue, Array
import queue
import sys
import time
import random

NUM_NODOS = 3
NODO_GLOTON = 0
RONDAS = 2               # EXPERIMENTO B: 2 rondas bastan, cada una del nodo 0 ya es muy larga
ENTRADAS_SEGUIDAS = 5    # EXPERIMENTO B: veces que el nodo 0 entra a la SC sin soltar el token
TIMEOUT_ESPERA = 5       # EXPERIMENTO B: cada 5 s los demás avisan cuánto llevan esperando


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
        time.sleep(3)  # el nodo glotón se demora más dentro de la SC (como en A)
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
            # EXPERIMENTO B: get() con timeout para mostrar cuánto tiempo lleva esperando
            inicio_espera = time.time()
            while True:
                try:
                    msg = queues[node_id].get(timeout=TIMEOUT_ESPERA)
                    break
                except queue.Empty:
                    esperado = int(time.time() - inicio_espera)
                    log(f"[Anillo] Nodo {node_id} lleva {esperado} s esperando el token")
            if msg == "TOKEN":
                has_token = True
                log(f"[Anillo] Nodo {node_id} recibe Token "
                    f"(esperó {time.time() - inicio_espera:.1f} s)")

        if node_id == NODO_GLOTON:
            # EXPERIMENTO B: el nodo 0 entra varias veces seguidas SIN pasar el token
            for vez in range(1, ENTRADAS_SEGUIDAS + 1):
                log(f"[Anillo] Nodo {node_id} retiene el token: entrada {vez}/{ENTRADAS_SEGUIDAS}")
                execute_critical_section(node_id, "Anillo-B")
                with entradas.get_lock():
                    entradas[node_id] += 1
                time.sleep(0.1)  # casi no espera entre una entrada y otra
        else:
            # Decidir si queremos la SC
            want_cs = random.choice([True, True, False])  # Probabilidad alta de quererla
            if want_cs:
                execute_critical_section(node_id, "Anillo-B")
                with entradas.get_lock():
                    entradas[node_id] += 1
            else:
                log(f"[Anillo] Nodo {node_id} no quiere la SC (want_cs = False)")

        # Pasar Token
        has_token = False
        next_node = (node_id + 1) % num_nodes
        # se registra antes del put() para que salga antes que el "recibe Token"
        log(f"[Anillo] Nodo {node_id} pasa Token a Nodo {next_node}")
        queues[next_node].put("TOKEN")

        if node_id == NODO_GLOTON:
            time.sleep(0.1)  # el nodo glotón casi no espera fuera de la SC
        else:
            time.sleep(random.uniform(0.5, 1.5))  # Simula trabajo fuera de SC


def run_experimento_b():
    print("--- Experimento B: Nodo que retiene el token ---", flush=True)
    queues = [Queue() for _ in range(NUM_NODOS)]
    entradas = Array('i', NUM_NODOS)  # contador compartido de entradas por nodo

    nodes = [Process(target=token_ring_node, args=(i, queues, NUM_NODOS, entradas, i == 0))
             for i in range(NUM_NODOS)]
    inicio = time.time()
    for n in nodes: n.start()
    for n in nodes: n.join()
    duracion = time.time() - inicio

    print("\n========== RESUMEN EXPERIMENTO B ==========", flush=True)
    for i in range(NUM_NODOS):
        etiqueta = " (retiene el token)" if i == NODO_GLOTON else ""
        print(f"Nodo {i}{etiqueta}: entró {entradas[i]} veces a la SC", flush=True)
    print(f"Duración total: {duracion:.1f} s", flush=True)
    print("Conclusión: mientras el nodo 0 retiene el token, los nodos 1 y 2 NO pueden "
          "entrar a la SC y esperan mucho tiempo (inanición / espera muy larga).", flush=True)


if __name__ == '__main__':
    run_experimento_b()
