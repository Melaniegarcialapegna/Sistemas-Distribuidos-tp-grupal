"""
Corre un escenario de prueba para las queries.

"""
import subprocess
import sys
import yaml 
import os
import compare #TODO
import results_q1
import argparse

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

# SE CALCULO LO ESPERADO, SE LO LEE Y SE COMPARA ----

def expected_q1(input_path):
    """ Lo que deberia devolver la Q1 para este parquet"""
    pass

RESULTS = {
    1: expected_q1,

}

class QueryResults:
    query: int
    status: str #OK, SKIP , FAIL
    rows_got : int=0 # filas que devolvio el cliente
    rows_expected : int=0 #filas que deberia haber devuelto
    message : str = "" #mensaje de error o advertencia
    details: dict = None #para fail 

def validate_client_results_in_query(client,query):
    """
    Valida los resultados de UNA QUERY para UN CLIENTE
    """
    results_query = RESULTS.get(query)

    if results_query is None:
        return QueryResults(query=query, status="SKIP", message=f"Query {query} no tiene resultados esperados definidos aun")

    try:
        expected_results = results_query(client.input_path)

    except FileNotFoundError as e:
        raise ScenarioError(f"{client.name}: no se pudo leer el archivo de entrada: {e.filename}")

    except ImportError as e:
        raise ScenarioError(f"{client.name}: falta pyarrow para leer parquet.\nInstalarlo con: pip install pyarrow") #TODO: ver si se puede solucionar esto

    output_path = os.path.join(client.output_path, f"result_Q{query}.csv")

    if not os.path.isfile(output_path):
        return QueryResults(query=query, status="FAIL",n_expected=sum(expected_results.values()) ,message=f"{client.name}: no se encontro el archivo de salida {output_path}")

    results_got = compare.read_rows(output_path)
    ok, datails = compare.COMPARATORS[query](results_got, expected_results)

    comparation_result = "OK" if ok else "FAIL"
    comparation_details = None if ok else datails

    return QueryResults(query=query, status=comparation_result, rows_got=sum(results_got.values()), rows_expected=sum(expected_results.values()), message=f"{client.name}: {query} {'OK' if ok else 'FAIL'}", details=comparation_details)


def print_query_result(r: QueryResults):
    if r.status == "OK":
        print(f"  q{r.query}: OK ({r.n_got} rows, {r.n_expected} expected)")
    elif r.status == "SKIP":
        print(f"  q{r.query}: SKIP ({r.message})")
    elif r.details is None:
        print(f"  q{r.query}: FAIL ({r.message})")
    else:
        d = r.details
        print(f"  q{r.query}: FAIL ({r.n_got} rows, {r.n_expected} expected)")
        print(f"    {sum(d['missing'].values())} faltantes, "
              f"{sum(d['unexpected'].values())} inesperadas, "
              f"{sum(d['duplicated'].values())} duplicadas")
        compare.show("    falta:      ", d["missing"])
        compare.show("    inesperada: ", d["unexpected"])
        compare.show("    duplicada:  ", d["duplicated"])

def validate_client_results(clients):
    """
    Valida todos los clientes e imprime un resumen de los resultados
    Devuelve true si no hubo ningun FAIL
    """
    all_ok, skipped = True, 0
    for client in clients:
        print(client.name)
        for query in client.queries:
            result = validate_client_results_in_query(client, query)
            print_query_result(result)
            if result.status == "FAIL":
                all_ok = False
            if result.status == "SKIP":
                skipped = skipped+ 1
    if not all_ok:
        print("RESULT: FAILED")
    elif skipped:
        print(f"RESULT: OK ({skipped} query(s) sin validar: NO TIENEN RESULTADOS ESPERADOS todavia)")
    else:
        print("RESULT: OK")
    return all_ok


def main():
    parser = argparse.ArgumentParser(description="Valida los resultados contra los resultados esperados")

    parser.add_argument("compose", help="ruta al docker-compose.yaml con el que se hizo la corrida")

    parser.add_argument("--wait", action="store_true", help="antes de validar espera a que terminen los clientes")

    parser.add_argument("--info", action="store_true", help="solo muestra que clientes hay y sus rutas (no valida)")


    parser.add_argument("--timeout", type=int, default=TIMEOUT_DEFAULT)
    
    args = parser.parse_args()
 
    try:
        clients = read_clients(args.compose)
        if args.info:
            for c in clients:
                print(f"{c.name}\n  input:   {c.input_path}\n  output:  {c.output_dir}\n  queries: {list(c.queries)}")
            return 0
        if args.wait:
            await_client_containers([c.name for c in clients], args.timeout)
        return 0 if validate_client_results(clients) else 1
    except ScenarioError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

if __name__ == "__main__":
    sys.exit(main())
