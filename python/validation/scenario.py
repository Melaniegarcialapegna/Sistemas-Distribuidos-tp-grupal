"""
Corre un escenario de prueba para las queries.

"""
import subprocess
import sys

TIMEOUT_DEFAULT = 1000 #segundos xd

class ScenarioError(Exception):
    "ERROR: Al correr los escnarios algo fallo (independientemente de que los resultados sean correctos o no)"

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

    exit_codes = result.stdout.strip()
    if len(exit_codes) != len(client_names):
        raise ScenarioError(f"Se esperaban {len(client_names)} exit codes pero se recibieron {len(exit_codes)}: {exit_codes}")

    #Se valida que cada exit code sea 0
    failed=[]
    for name, code in zip(client_names, exit_codes):
        if code != "0":
            failed.append(f"{name} with code {code}")
    if failed: 
        raise ScenarioError("Clientes que terminaron con error: " + ", ".join(failed))

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
