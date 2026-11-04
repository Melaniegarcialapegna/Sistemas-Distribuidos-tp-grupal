"""
Corre un escenario de prueba para las queries.

Los resultados esperados son ESTATICOS (carpeta expected/, generada una vez con results_qN.py).
Este script nunca los recalcula: solo los lee y compara contra lo que escribieron los clientes.


Main tiene tres modos de ejecucion:
- python3 validation/scenario.py docker-compose.yaml  -> valida los resultados (asumiendo que ya se corrieron las cosas y los resultados ya estan en los csv)

- python3 validation/scenario.py docker-compose.yaml --info -> solo muestra que clientes hay y sus rutas (no valida). Unicamente es para ver que el compose esta bien armado.

RECOMENDADA:) (el resto sirven mas para debuggear que otra cosa)

- python3 validation/scenario.py docker-compose.yaml --run -> es una combinacion de las dos primeras opciones. Espera a los clientes y luego valida los resultados. Se puede usar para correr todo de una vez.

Ademas limpia el output/ de corridas anteriores, levanta el compose espera a los clientes, valida y vuelve a bakar todo.


Los exit codes son:
0: todo OK
1: hubo algun FAIL en los resultados de los clientes
2: hubo un error en la ejecucion del escenario (ej: no se encontro el docker-compose.yaml, algun cliente termino con error, input no existe, etc..)


"""
from dataclasses import dataclass
import shutil
import subprocess
import sys
import yaml 
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import compare 
import argparse

TIMEOUT_DEFAULT = 1000 #segundos xd
EXPECTED_DIR_DEFAULT = "expected" #carpeta con resultados
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
@dataclass
class ClientConfig:
    """
    Tiene la info que se necesita del cliente para poder validar sus resultados 
    """
    name : str 
    container_name: str
    input_file: str #la carpeta que se le da al cliente como input
    output_dir: str #carpeta en donde cliente escribe los resultados
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
    return services

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
    if isinstance(environment, dict):
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

        if not host_dir.startswith((".","/","~")): #no es una ruta valida
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
        raise ScenarioError(f"QUERIES invalido: {value}.\nDebe ser una lista de numeros separados por coma, ej: 1,2,3,4,5")

def read_client_config(name, service, compose_dir):
    """
    Arma el ClientConfig de un cliente a partir de su seccion en el compose.
    """ 
    environment = service.get("environment")
    volumes = service.get("volumes")
    container_name = service.get("container_name", name)

    input_file = find_enviroment_variable(environment, "INPUT_FILE")
    if not input_file:
        raise ScenarioError(f"{name}: falta la variable de entorno INPUT_FILE")

    output_dir = find_enviroment_variable(environment, "OUTPUT_DIR") or "/output"

    queries_val = find_enviroment_variable(environment, "QUERIES") or find_enviroment_variable(environment, "EXPECTED_QUERIES")

    return ClientConfig(
        name=name,
        container_name=container_name,
        input_file=container_path_to_host(input_file, volumes, compose_dir),
        output_dir=container_path_to_host(output_dir, volumes, compose_dir),
        queries=parse_queries(queries_val)
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

# SE COMPARA ----
@dataclass
class QueryResults:
    query: int
    status: str #OK, SKIP , FAIL
    rows_got : int=0 # filas que devolvio el cliente
    rows_expected : int=0 #filas que deberia haber devuelto
    message : str = "" #mensaje de error o advertencia
    details: dict = None #para fail 

def validate_client_results_in_query(client,query,expected_dir):
    """
    Valida los resultados de UNA QUERY para UN CLIENTE
    """
    comparator = compare.COMPARATORS.get(query)

    if comparator is None:
        return QueryResults(query=query, status="SKIP", message=f"Query {query} no tiene resultados definidos para comparar aun")

    expected_path = compare.expected_file(expected_dir, client.input_file, query)
    if not os.path.isfile(expected_path):
        raise ScenarioError(
            f"{client.name}: falta el archivo esperado {expected_path}.\n"
            f"Generarlo con: python3 validation/results_q{query}.py {expected_dir} {client.input_file}")
    expected_results = compare.read_rows(expected_path, query=query)

    output_path = os.path.join(client.output_dir, f"result_Q{query}.csv")
 
    if not os.path.isfile(output_path):
        return QueryResults(query=query, status="FAIL", rows_expected=sum(expected_results.values()), message=f"{client.name}: no se encontro el archivo de salida {output_path}")
    
    results_got = compare.read_rows(output_path, query=query)
    ok, details = compare.COMPARATORS[query](results_got, expected_results)

    comparation_result = "OK" if ok else "FAIL"
    comparation_details = None if ok else details

    return QueryResults(query=query, status=comparation_result, rows_got=sum(results_got.values()), rows_expected=sum(expected_results.values()), message=f"{client.name}: {query} {'OK' if ok else 'FAIL'}", details=comparation_details)


def print_query_result(r: QueryResults):
    if r.status == "OK":
        print(f"  q{r.query}: OK ({r.rows_got} rows, {r.rows_expected} expected)")
    elif r.status == "SKIP":
        print(f"  q{r.query}: SKIP ({r.message})")
    elif r.details is None:
        print(f"  q{r.query}: FAIL ({r.message})")
    else:
        d = r.details
        print(f"  q{r.query}: FAIL ({r.rows_got} rows, {r.rows_expected} expected)")
        print(f"    {sum(d['missing'].values())} faltantes, "
              f"{sum(d['unexpected'].values())} inesperadas, "
              f"{sum(d['duplicated'].values())} duplicadas")
        compare.show("    falta:      ", d["missing"])
        compare.show("    inesperada: ", d["unexpected"])
        compare.show("    duplicada:  ", d["duplicated"])

def validate_client_results(clients,expected_dir):
    """
    Valida todos los clientes e imprime un resumen de los resultados
    Devuelve true si no hubo ningun FAIL
    """
    all_ok, skipped = True, 0
    for client in clients:
        print(client.name)
        for query in client.queries:
            result = validate_client_results_in_query(client, query,expected_dir)
            print_query_result(result)
            if result.status == "FAIL":
                all_ok = False
            if result.status == "SKIP":
                skipped = skipped+ 1
    if not all_ok:
        print("RESULT: FAILED")
    elif skipped:
        print(f"RESULT: OK ({skipped} query(s) sin validar: NO TIENEN RESULTADOS PARA COMPARAR todavia)")
    else:
        print("RESULT: OK")
    return all_ok

# PARA LA CORRIDA DE RUN (todo el escenario )----

def run_docker(command, error_message=None):
    """
    Corre un comando de docker mostrando lo que devuelve por pantalla.4
    Si error_menssage ses None se ignora si falla, sino el se lanza un ScenarioError con ese mensaje
    """

    try:
        result = subprocess.run(command)
    except FileNotFoundError:
        raise ScenarioError("No se encontro el comando 'docker'")
    if error_message is not None and result.returncode != 0:
        raise ScenarioError(error_message)

def clenup_output_dir(output_dir):
    """
    Deja vacia la carpeta de salida del cliente (output_dir).
    Esto pq los clientes escriben en modo append y sino se van a ir acumulando los resultados de las distintas corridas.
    """
    if os.path.isdir(output_dir):
        try:
            shutil.rmtree(output_dir)
        except PermissionError:
            # Como los archivos se crean desde el contenedor se borran desde otr
            parent , name = os.path.split(os.path.abspath(output_dir))
            run_docker(["docker", "run", "--rm", "-v", f"{parent}:/w", "alpine", "sh", "-c", f"rm -rf /w/{name}"], f"No se pudo limpiar {output_dir}.\n BORRAR DE MANERA MANUAL!!!")
    
    os.makedirs(output_dir, exist_ok=True)



def run_scenario(compose_path, expected_dir, timeout=TIMEOUT_DEFAULT):
    """
    Corre el escnario completo y devuelve trye si los resultados son correctos.
    """

    clients = read_clients(compose_path)
    container_names = [client.container_name for client in clients]

    compose = ["docker","compose", "-f", compose_path]

    print(">>Limpiando las carpetas de salida...")
    for client in clients:
        clenup_output_dir(client.output_dir)

    print(">>Bajando restos de corridas anteriores...")
    run_docker([*compose, "down", "-v", "--remove-orphans"])

    try:
        print(f">>Levantando {compose_path}...")
        run_docker([*compose, "up", "-d"], f"No se pudo levantar {compose_path}")

        print(f">> Esperando a: {', '.join(container_names)} (timeout {timeout}s)")
        try:
            await_client_containers(container_names, timeout)
        except ScenarioError as e:
            print(f"Los clientes no terminaron correctamente!!\nERROR:{e}\nUltimos logs:")
            run_docker([*compose, "logs", "--tail=20", *container_names,"gateway"])
            raise

        print(">>Validando resultados...")
        return validate_client_results(clients,expected_dir)

    finally:
        #Pase lo que pase no se deja a ningun conetenedor levantado
        print(">>Bajando contenedores...")
        run_docker([*compose, "down", "-v", "--remove-orphans"])
        

def resolve_expected_dir(expected_dir, compose_path):
    compose_dir = os.path.dirname(os.path.abspath(compose_path))
    candidate = os.path.join(compose_dir, expected_dir)
    if os.path.isdir(candidate):
        return candidate
    if os.path.isdir(expected_dir):
        return expected_dir
    return expected_dir

def main():
    parser = argparse.ArgumentParser(description="Valida los resultados contra los resultados esperados")

    parser.add_argument("compose", help="ruta al docker-compose.yaml con el que se hizo la corrida")

    parser.add_argument("--info", action="store_true", help="solo muestra que clientes hay y sus rutas (no valida)")

    parser.add_argument("--run", action="store_true", help="corre todo: limpia output, levanta el compose, espera, valida y baja todo")

    parser.add_argument("--timeout", type=int, default=TIMEOUT_DEFAULT)
    
    parser.add_argument("--expected", default=EXPECTED_DIR_DEFAULT, help="carpeta con los resultados esperados (por defecto: expected)")
    
    args = parser.parse_args()
    args.expected = resolve_expected_dir(args.expected, args.compose)
 
    try:
        if args.run:
            return 0 if run_scenario(args.compose, args.expected, args.timeout) else 1
 
        clients = read_clients(args.compose)
        if args.info:
            for c in clients:
                print(f"{c.name} (container: {c.container_name})\n  input:   {c.input_file}\n  output:  {c.output_dir}\n  queries: {list(c.queries)}")
                print(f"  esperado Q1: {compare.expected_file(args.expected, c.input_file, 1)}")
            return 0

        # Modo por defecto (sin --run ni --info): validar resultados ya generados
        return 0 if validate_client_results(clients, args.expected) else 1
 
    except ScenarioError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

if __name__ == "__main__":
    sys.exit(main())
