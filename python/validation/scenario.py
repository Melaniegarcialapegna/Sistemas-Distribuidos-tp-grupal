"""
Corre un escenario de prueba para las queries.

"""
import subprocess
import sys
import yaml 
import os

TIMEOUT_DEFAULT = 1000 #segundos xd
ALL_QUERIES = (1,2,3,4,5)

class ScenarioError(Exception):
    "ERROR: Al correr los escnarios algo fallo (independientemente de que los resultados sean correctos o no)"


# ESPERAR A LOS CLIENTES ----

def await_client_containers(client_names, timeout=TIMEOUT_DEFAULT):
    """
    Bloquea hasta que todos los contenedores de 'client_names' terminen de ejecutarse y verifica que cada uno de ellos hayan salid con exit code 0. 
    En caso contrario lanza un ScenarioError.
    """
    if not client_names:
        raise ScenarioError("No hay clientes a los cuales esperar")

    command = ["docker", "container", "wait", *client_names] #docker container wait c1 c2 ... cn

    #Se queda bloqueado hasta que cada contnedor termina
    try:
        result = subprocess.run(command, capture_output=True,text=True,timeout=timeout)
    except subprocess.TimeoutExpired:
        raise ScenarioError(f"Los clientes no terminaron en {timeout} segundos. Se aborta la ejecucion del escenario ya que el sistema se 'colgo'")
    except FileNotFoundError:
        raise ScenarioError("No se encontro el comando 'docker'")

    # Si falla el comand en si
    if result.returncode != 0:
        raise ScenarioError(f"Docker container wait fallo: {result.stderr.strip()}")

    exit_codes = result.stdout.split()
    if len(exit_codes) != len(client_names):
        raise ScenarioError(f"Se esperaban {len(client_names)} exit codes pero se recibieron {len(exit_codes)}: {exit_codes}")

    #Se valida que cada exit code sea 0
    failed=[]
    for name, code in zip(client_names, exit_codes):
        if code != "0":
            failed.append(f"{name} with code {code}")
    if failed: 
        raise ScenarioError("Clientes que terminaron con error: " + ", ".join(failed))


# LEER DEL docker-compose----

class ClientConfig:
    """
    Tiene la info que se necesita del cliente para poder validar sus resultados 
    """
    name : str 
    input_path: str #la carpeta que se le da al cliente como input
    output_path: str #carpeta en donde cliente escribe los resultados
    queries: tuple #queries que se espera que haya respondido

def load_services(compose_path):
    """
    Lee el .yaml y devuelve un dicc de la forma {nombre_servicio: configuracion}
    """
    try:
        with open(compose_path) as f:
            compose = yaml.safe_load(f)
    except FileNotFoundError:
        raise ScenarioError(f"No se encontro el archivo {compose_path}")
    except yaml.YAMLError as e:
        raise ScenarioError(f"El compose no es un YAML valido: {e}")

    services = compose.get("services") if isinstance(compose, dict) else None
    if not isinstance(services, dict) or not services:
        raise ScenarioError(f"{compose_path} no tiene la seccion 'services'")

def find_client_names(services):
    """
    Los clientes son los servicios que se construyen a partir de client
    """
    names = []
    for name, service in services.items():
        build = service.get("build")
        dockerfile = build.get("dockerfile","") if isinstance(build, dict) else ""
        if "client" in dockerfile: names.append(name)
    return names

def find_enviroment_variable(environment,target):
    """
    Busca el valor de la variable target en el enviroment de un servicio.
    Devuelve None si no se encuentra. 
    """
    if isinstance(environment, list):
        value = environment.get(target)
        return None if value is None else str(value)
    for entry in environment or []:
        name, _,value = entry.partition("=")
        if name == target: return value
    return None 


def container_path_to_host(container_path,volumes, compose_dir):
    """
    Convierte una ruta que esta adentro del contenedor a una ruta afuera del contenedor (en el host / mi compu).
    """
    best = None
    for volume in volumes or []:
        if not isinstance(volume, str): continue
        parts = volume.split(":")
        
        if len(parts) < 2: continue

        host_dir,container_dir = parts[0], parts[1].rstrip("/")

        if not host_dir.startswith(".","/","~"): #no es una ruta valida
            continue

        if container_path == container_dir or container_path.startswith(container_dir + "/"):
            if best is None or len(container_dir) > len(best[1]):
                best = (host_dir, container_dir)

        if best is None:
            raise ScenarioError(f"La ruta {container_path} no esta montada con ningun volumen del contenedor.")

        host_dir,container_dir = best
        rest = container_path[len(container_dir):].lstrip("/")
        return os.path.normpath(os.path.join(compose_dir, os.path.expanduser(host_dir), rest))

def parse_queries(value):
    if not value: return ALL_QUERIES

    try:
        queries = []
        for q in value.split(","):
            if q.strip() == "": continue
            queries.append(int(q.strip()))
        return tuple(queries)
    
    except ValueError:
        raise ScenarioError(f"EXPECTED_QUERIES invalido: {value}.\nDebe ser una lista de numeros separados por coma, ej: 1,2,3,4,5")

def read_client_config(name, service, compose_dir):
    """
    Arma el ClientConfig de un cliente a partir de su ccion en el compose.
    """ 
    environment = service.get("environment")
    volumes = service.get("volumes")

    input_path = find_enviroment_variable(environment, "INPUT_PATH")
    if not input_path:
        raise ScenarioError(f"{name}: falta la variable de entorno INPUT_PATH")

    output_path = find_enviroment_variable(environment, "OUTPUT_PATH")
    if not output_path:
        raise ScenarioError(f"{name}: falta la variable de entorno OUTPUT_PATH")

    return ClientConfig(
        name=name,
        input_path=container_path_to_host(input_path,volumes, compose_dir),
        output_path=container_path_to_host(output_path,volumes, compose_dir),
        queries=parse_queries(find_enviroment_variable(environment, "EXPECTED_QUERIES"))
    )

def read_clients(compose_path):
    """
    Devuelve una lista de ClientConfig de todos los clientes que estan en el compose.
    """
    services = load_services(compose_path)
    client_names = find_client_names(services)
    if not client_names:
        raise ScenarioError(f"{compose_path} no tiene ningun servicio cliente")
    compose_dir = os.path.dirname(os.path.abspath(compose_path))
    return [read_client_config(name, services[name], compose_dir) for name in client_names]


def main():
    client_names = sys.argv[1:]
    try:
        await_client_containers(client_names)
    except ScenarioError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    print(f"Todos los clientes terminaron correctamente: {', '.join(client_names)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
