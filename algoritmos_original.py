# VERSIÓN ORIGINAL (SIN CORREGIR) del código de la guía.
# Solo sirve para mostrar que Ricart y Agrawala se puede quedar colgado (captura 06).
# Único cambio: la opción del menú se puede pasar como argumento para correrla en Docker.
import multiprocessing
from multiprocessing import Process, Queue
import time
import random
import sys

# ==========================================
# SECCIÓN CRÍTICA SIMULADA
# ==========================================
def execute_critical_section(node_id, algo_name):
    print(f"[{algo_name}] NODO {node_id} ENTRANDO a la Sección Crítica...")
    time.sleep(random.uniform(0.5, 1.5))  # Simula trabajo en SC
    print(f"[{algo_name}] NODO {node_id} SALIENDO de la Sección Crítica.")

# ==========================================
# 1. ALGORITMO SERVIDOR CENTRALIZADO
# ==========================================
def centralized_server(request_q, grant_queues, release_q, num_nodes):
    granted_to = None
    while True:
        # Revisar si alguien liberó
        if not release_q.empty():
            released_node = release_q.get()
            granted_to = None
            print(f"[Centralizado] Servidor: Nodo {released_node} liberó la SC.")

        # Si la SC está libre, otorgar al siguiente
        if granted_to is None and not request_q.empty():
            req_node = request_q.get()
            granted_to = req_node
            grant_queues[req_node].put("GRANT")
            print(f"[Centralizado] Servidor: Otorgando SC a Nodo {req_node}")

def centralized_node(node_id, request_q, grant_q, release_q):
    for _ in range(2):  # Cada nodo entra 2 veces a la SC
        time.sleep(random.uniform(1, 4))
        print(f"[Centralizado] Nodo {node_id} solicitando SC...")
        request_q.put(node_id)

        msg = grant_q.get()  # Bloquea hasta recibir GRANT
        if msg == "GRANT":
            execute_critical_section(node_id, "Centralizado")
            release_q.put(node_id)

def run_centralized():
    num_nodes = 3
    request_q = Queue()
    release_q = Queue()
    grant_queues = [Queue() for _ in range(num_nodes)]

    server = Process(target=centralized_server, args=(request_q, grant_queues, release_q, num_nodes))
    server.start()

    nodes = [Process(target=centralized_node, args=(i, request_q, grant_queues[i], release_q)) for i in range(num_nodes)]
    for n in nodes: n.start()
    for n in nodes: n.join()

    time.sleep(1)
    server.terminate()

# ==========================================
# 2. ALGORITMO RICART Y AGRAWALA
# ==========================================
def ricart_agrawala_node(node_id, queues, num_nodes):
    clock = 0
    state = "RELEASED"
    replies_received = 0
    deferred = []

    for _ in range(2):
        time.sleep(random.uniform(1, 4))
        # Solicitar SC
        state = "WANTED"
        clock += 1
        my_req_time = clock
        print(f"[R&A] Nodo {node_id} solicita SC (Tiempo lógico: {my_req_time})")

        for i in range(num_nodes):
            if i != node_id:
                queues[i].put(("REQUEST", node_id, my_req_time))

        # Esperar N-1 REPLY
        while replies_received < num_nodes - 1:
            msg_type, src_id, req_time = queues[node_id].get()
            if msg_type == "REPLY":
                replies_received += 1
            elif msg_type == "REQUEST":
                clock = max(clock, req_time) + 1
                if my_req_time < req_time or (my_req_time == req_time and node_id < src_id):
                    # Tenemos prioridad, diferimos la respuesta
                    deferred.append(src_id)
                else:
                    # El otro tiene prioridad, respondemos
                    queues[src_id].put(("REPLY", node_id, clock))

        # Entrar a SC
        state = "HELD"
        execute_critical_section(node_id, "R&A")

        # Salir de SC
        state = "RELEASED"
        replies_received = 0
        for dest in deferred:
            queues[dest].put(("REPLY", node_id, clock))
        deferred.clear()

    # Limpiar cola residual para que los procesos terminen limpiamente
    time.sleep(2)
    while not queues[node_id].empty():
        queues[node_id].get()

# ==========================================
# 3. ALGORITMO ANILLO DE TOKEN
# ==========================================
def token_ring_node(node_id, queues, num_nodes, initial_token=False):
    has_token = initial_token

    for _ in range(2):
        if not has_token:
            # Esperar Token
            msg = queues[node_id].get()
            if msg == "TOKEN":
                has_token = True

        # Decidir si queremos la SC
        want_cs = random.choice([True, True, False])  # Probabilidad alta de quererla
        if want_cs:
            execute_critical_section(node_id, "Anillo")

        # Pasar Token
        has_token = False
        next_node = (node_id + 1) % num_nodes
        queues[next_node].put("TOKEN")
        print(f"[Anillo] Nodo {node_id} pasa Token a Nodo {next_node}")

        time.sleep(random.uniform(0.5, 1.5))  # Simula trabajo fuera de SC

def run_token_ring():
    num_nodes = 3
    queues = [Queue() for _ in range(num_nodes)]

    # El nodo 0 inicia con el token
    nodes = [Process(target=token_ring_node, args=(i, queues, num_nodes, i == 0)) for i in range(num_nodes)]
    for n in nodes: n.start()
    for n in nodes: n.join()

# ==========================================
# MENÚ PRINCIPAL
# ==========================================
if __name__ == '__main__':
    print("--- Práctica de Exclusión Mutua Distribuida ---")
    print("1. Servidor Centralizado")
    print("2. Ricart y Agrawala")
    print("3. Anillo de Token")

    # Único cambio: aceptar la opción por argumento (python algoritmos_original.py 2)
    choice = sys.argv[1] if len(sys.argv) > 1 else input("Seleccione el algoritmo a ejecutar (1-3): ")
    sys.stdout.flush()

    if choice == '1':
        run_centralized()
    elif choice == '2':
        # Ejecutar R&A
        num_nodes = 3
        queues = [Queue() for _ in range(num_nodes)]
        nodes = [Process(target=ricart_agrawala_node, args=(i, queues, num_nodes)) for i in range(num_nodes)]
        for n in nodes: n.start()
        for n in nodes: n.join()
    elif choice == '3':
        run_token_ring()
    else:
        print("Opción no válida.")
