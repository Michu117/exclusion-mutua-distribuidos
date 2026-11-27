import multiprocessing
from multiprocessing import Process, Queue
import threading  # CORRECCIÓN: se usa un hilo receptor por nodo en Ricart y Agrawala
import time
import random
import sys
import os  # CORRECCIÓN: para leer la opción desde la variable de entorno ALGORITMO


# ==========================================
# FUNCIÓN AUXILIAR DE REGISTRO
# ==========================================
# CORRECCIÓN: cada mensaje lleva marca de tiempo HH:MM:SS.mmm para que se note la concurrencia
def log(msg):
    ahora = time.time()
    milisegundos = int((ahora % 1) * 1000)
    marca = time.strftime("%H:%M:%S", time.localtime(ahora)) + f".{milisegundos:03d}"
    # CORRECCIÓN: la línea completa se escribe de una sola vez para que dos procesos
    # no mezclen sus mensajes en la misma línea
    sys.stdout.write(f"{marca} {msg}\n")
    sys.stdout.flush()


# ==========================================
# SECCIÓN CRÍTICA SIMULADA
# ==========================================
def execute_critical_section(node_id, algo_name):
    log(f"[{algo_name}] NODO {node_id} ENTRANDO a la Sección Crítica...")
    time.sleep(random.uniform(0.5, 1.5))  # Simula trabajo en SC
    log(f"[{algo_name}] NODO {node_id} SALIENDO de la Sección Crítica.")


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
            log(f"[Centralizado] Servidor: Nodo {released_node} liberó la SC.")

        # Si la SC está libre, otorgar al siguiente
        if granted_to is None and not request_q.empty():
            req_node = request_q.get()
            granted_to = req_node
            grant_queues[req_node].put("GRANT")
            log(f"[Centralizado] Servidor: Otorgando SC a Nodo {req_node}")

        # CORRECCIÓN: pausa corta para evitar la espera activa (el bucle usaba el 100 % de CPU)
        time.sleep(0.01)


def centralized_node(node_id, request_q, grant_q, release_q):
    for _ in range(2):  # Cada nodo entra 2 veces a la SC
        time.sleep(random.uniform(1, 4))
        log(f"[Centralizado] Nodo {node_id} solicitando SC...")
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
    my_req_time = 0
    replies_received = 0
    deferred = []

    # CORRECCIÓN: candado porque el hilo principal y el hilo receptor comparten el estado del nodo
    lock = threading.Lock()
    # CORRECCIÓN: evento que avisa al hilo principal cuando llegaron los N-1 REPLY
    all_replies = threading.Event()

    # CORRECCIÓN: hilo receptor que atiende los mensajes TODO el tiempo (también en HELD,
    # mientras duerme y después de terminar sus rondas); antes solo se atendían en WANTED
    def receiver():
        nonlocal clock, replies_received
        finished_nodes = 0
        while True:
            msg_type, src_id, msg_time = queues[node_id].get()
            with lock:
                # Regla de Lamport al recibir: max(local, recibido) + 1
                clock = max(clock, msg_time) + 1

                if msg_type == "REQUEST":
                    # CORRECCIÓN: el REQUEST se decide con el estado ACTUAL del nodo (reglas de R&A)
                    if state == "HELD":
                        deferred.append(src_id)
                        log(f"[R&A] Nodo {node_id} difiere REPLY a Nodo {src_id} (estoy en la SC)")
                    elif state == "WANTED" and (my_req_time, node_id) < (msg_time, src_id):
                        # Tenemos prioridad, diferimos la respuesta
                        deferred.append(src_id)
                        log(f"[R&A] Nodo {node_id} difiere REPLY a Nodo {src_id} "
                            f"(yo=({my_req_time},{node_id}) < él=({msg_time},{src_id}))")
                    else:
                        # RELEASED, o el otro tiene prioridad: respondemos de inmediato
                        queues[src_id].put(("REPLY", node_id, clock))
                        log(f"[R&A] Nodo {node_id} envía REPLY a Nodo {src_id}")

                elif msg_type == "REPLY":
                    replies_received += 1
                    if replies_received == num_nodes - 1:
                        all_replies.set()

                elif msg_type == "DONE":
                    # CORRECCIÓN: un nodo avisa que terminó (también este mismo nodo); cuando
                    # TODOS terminaron ya nadie va a pedir la SC y el receptor puede cerrarse
                    finished_nodes += 1
                    if finished_nodes == num_nodes:
                        return

    receptor = threading.Thread(target=receiver)
    receptor.start()

    for _ in range(2):
        time.sleep(random.uniform(1, 4))
        # Solicitar SC
        with lock:
            state = "WANTED"
            clock += 1
            my_req_time = clock
            replies_received = 0
            all_replies.clear()
            log(f"[R&A] Nodo {node_id} solicita SC (Tiempo lógico: {my_req_time})")

            for i in range(num_nodes):
                if i != node_id:
                    queues[i].put(("REQUEST", node_id, my_req_time))

        # Esperar N-1 REPLY (los cuenta el hilo receptor)
        all_replies.wait()

        # Entrar a SC
        with lock:
            state = "HELD"
        execute_critical_section(node_id, "R&A")

        # Salir de SC: responder a todos los que se difirieron
        with lock:
            state = "RELEASED"
            for dest in deferred:
                queues[dest].put(("REPLY", node_id, clock))
                log(f"[R&A] Nodo {node_id} envía REPLY diferido a Nodo {dest}")
            deferred.clear()

    # CORRECCIÓN: en vez de vaciar la cola sin responder, se avisa a los demás que este nodo
    # terminó y se sigue respondiendo (hilo receptor) hasta que todos terminen
    # (el DONE también va a la propia cola para que el receptor no se cierre antes de tiempo)
    with lock:
        for i in range(num_nodes):
            queues[i].put(("DONE", node_id, clock))
    receptor.join()
    log(f"[R&A] Nodo {node_id} terminó sus rondas y todos los nodos ya terminaron.")


def run_ricart_agrawala():
    num_nodes = 3
    queues = [Queue() for _ in range(num_nodes)]
    nodes = [Process(target=ricart_agrawala_node, args=(i, queues, num_nodes)) for i in range(num_nodes)]
    for n in nodes: n.start()
    for n in nodes: n.join()


# ==========================================
# 3. ALGORITMO ANILLO DE TOKEN
# ==========================================
def token_ring_node(node_id, queues, num_nodes, initial_token=False):
    has_token = initial_token
    if has_token:
        log(f"[Anillo] Nodo {node_id} inicia con el Token")

    for _ in range(2):
        if not has_token:
            # Esperar Token
            msg = queues[node_id].get()
            if msg == "TOKEN":
                has_token = True
                # CORRECCIÓN: se registra cuándo llega el token para ver el recorrido del anillo
                log(f"[Anillo] Nodo {node_id} recibe Token")

        # Decidir si queremos la SC
        want_cs = random.choice([True, True, False])  # Probabilidad alta de quererla
        if want_cs:
            execute_critical_section(node_id, "Anillo")
        else:
            # CORRECCIÓN: se muestra cuando el nodo no quiere la SC y solo pasa el token
            log(f"[Anillo] Nodo {node_id} no quiere la SC (want_cs = False)")

        # Pasar Token
        has_token = False
        next_node = (node_id + 1) % num_nodes
        # CORRECCIÓN: se registra antes del put() para que salga antes que el "recibe Token"
        log(f"[Anillo] Nodo {node_id} pasa Token a Nodo {next_node}")
        queues[next_node].put("TOKEN")

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

    # CORRECCIÓN: la opción se puede pasar por argumento o por la variable ALGORITMO;
    # si no llega ninguna, se pregunta con input() como en la guía
    if len(sys.argv) > 1:
        choice = sys.argv[1]
        print(f"Opción recibida por argumento: {choice}")
    elif os.environ.get("ALGORITMO"):
        choice = os.environ["ALGORITMO"]
        print(f"Opción recibida por variable ALGORITMO: {choice}")
    else:
        choice = input("Seleccione el algoritmo a ejecutar (1-3): ")
    choice = choice.strip()
    sys.stdout.flush()

    if choice == '1':
        run_centralized()
    elif choice == '2':
        # Ejecutar R&A
        run_ricart_agrawala()
    elif choice == '3':
        run_token_ring()
    else:
        print("Opción no válida.")
