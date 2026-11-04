#!/usr/bin/env python3
import argparse
import os
import sys

QUERY_NODES = {
    1: {"YEAR_FILTER", "LANGUAGE_FILTER"},
    2: {"YEAR_FILTER", "BIOGRAPHY_FILTER", "IMAGE_DOWNLOADER"},
    3: {"TOKENIZER", "UNIQUE_WORDS_FILTER", "HASHER", "WORD_COUNTER"},
    4: {"TOKENIZER", "UNIQUE_WORDS_FILTER", "HASHER", "WORD_COUNTER", "JOIN_TOP_20"},
    5: {"TOKENIZER", "SUM", "SUM_JOIN", "AVERAGE_FILTER"},
}

NODE_DISPLAY_NAMES = {
    "YEAR_FILTER": "Year Filter",
    "LANGUAGE_FILTER": "Language Filter",
    "BIOGRAPHY_FILTER": "Biography Filter",
    "IMAGE_DOWNLOADER": "Image Downloader",
    "TOKENIZER": "Tokenizer",
    "UNIQUE_WORDS_FILTER": "Unique Words Filter",
    "HASHER": "Hasher",
    "WORD_COUNTER": "Word Counter",
    "JOIN_TOP_20": "Join Top 20",
    "SUM": "Sum",
    "SUM_JOIN": "Sum Join",
    "AVERAGE_FILTER": "Average Filter",
}

NODE_ORDER = [
    "YEAR_FILTER",
    "LANGUAGE_FILTER",
    "BIOGRAPHY_FILTER",
    "IMAGE_DOWNLOADER",
    "TOKENIZER",
    "UNIQUE_WORDS_FILTER",
    "HASHER",
    "WORD_COUNTER",
    "JOIN_TOP_20",
    "SUM",
    "SUM_JOIN",
    "AVERAGE_FILTER",
]


def generate_rabbitmq():
    return """  rabbitmq:
    build:
      context: ./src/rabbitmq
      dockerfile: Dockerfile
    container_name: rabbitmq
    environment:
    - RABBITMQ_LOG_LEVELS=error
    healthcheck:
      interval: 5s
      retries: 10
      start_period: 50s
      test: rabbitmq-diagnostics check_port_connectivity
      timeout: 3s
    ports:
    - 5672:5672
    - 15672:15672"""


def generate_gateway(wanted_queries, replicas):
    queries_str = ",".join(str(q) for q in sorted(wanted_queries))
    env_lines = [
        "    - RABBITMQ_HOST=rabbitmq",
        "    - OUTPUT_QUEUE=input_queue",
        "    - PYTHONUNBUFFERED=1",
        "    - SERVER_HOST=gateway",
        "    - SERVER_PORT=5678",
        "    - RESULTS_QUEUE=results_queue",
        "    - TOKENIZER_QUEUE=tokenizer_queue",
        "    - YEAR_FILTER_QUEUE=year_filter_queue",
        f"    - QUERIES={queries_str}",
    ]
    if 1 in wanted_queries:
        env_lines.append(f"    - Q1_EOF_EXPECTED={replicas.get('LANGUAGE_FILTER', 1)}")
    if 2 in wanted_queries:
        env_lines.append(f"    - Q2_EOF_EXPECTED={replicas.get('IMAGE_DOWNLOADER', 1)}")
    if 3 in wanted_queries:
        env_lines.append(f"    - Q3_EOF_EXPECTED={replicas.get('WORD_COUNTER', 1)}")
    if 4 in wanted_queries:
        env_lines.append(f"    - Q4_EOF_EXPECTED={replicas.get('JOIN_TOP_20', 1)}")
    if 5 in wanted_queries:
        env_lines.append(f"    - Q5_EOF_EXPECTED={replicas.get('AVERAGE_FILTER', 1)}")

    env_block = "\n".join(env_lines)
    return f"""  gateway:
    build:
      context: ./src/
      dockerfile: gateway/Dockerfile
    container_name: gateway
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
{env_block}"""


def generate_client(wanted_queries, client_depends_on, row_batch_size=1000):
    queries_str = ",".join(str(q) for q in sorted(wanted_queries))
    dep_lines = "\n".join(f"    - {dep}" for dep in client_depends_on)
    return f"""  client_0:
    build:
      context: ./src/
      dockerfile: client/Dockerfile
    container_name: client_0
    depends_on:
{dep_lines}
    environment:
    - INPUT_FILE=/datasets
    - PYTHONUNBUFFERED=1
    - SERVER_HOST=gateway
    - SERVER_PORT=5678
    - ROW_BATCH_SIZE={row_batch_size}
    - QUERIES={queries_str}
    volumes:
    - ./datasets:/datasets
    - ./output:/output"""


def generate_year_filters(count):
    blocks = []
    for i in range(count):
        blocks.append(f"""  year_filter_{i}:
    build:
      context: ./src/
      dockerfile: year_filter/Dockerfile
    container_name: year_filter_{i}
    depends_on:
    - gateway
    environment:
    - BATCH_SIZE=100
    - REPLICA_ID={i}
    - INPUT_QUEUE_NAME=year_filter_queue
    - Q1_OUTPUT_QUEUE_NAME=article_language_queue
    - Q2_OUTPUT_QUEUE_NAME=article_sections_queue
    - CONTROL_EXCHANGE_NAME=year_filter_control
    - CONTROL_ROUTING_KEY=
    - RABBITMQ_HOST=rabbitmq
    - PREVIOUS_STAGE_REPLICAS=1
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_language_filters(count, prev_replicas):
    deps = "\n".join(f"    - year_filter_{j}" for j in range(prev_replicas))
    blocks = []
    for i in range(count):
        blocks.append(f"""  language_filter_{i}:
    build:
      context: ./src/
      dockerfile: language_filter/Dockerfile
    container_name: language_filter_{i}
    depends_on:
{deps}
    environment:
    - BATCH_SIZE=100
    - REPLICA_ID={i}
    - INPUT_QUEUE_NAME=article_language_queue
    - Q1_OUTPUT_QUEUE_NAME=results_queue
    - CONTROL_EXCHANGE_NAME=language_filter_control
    - CONTROL_ROUTING_KEY=
    - RABBITMQ_HOST=rabbitmq
    - PREVIOUS_STAGE_REPLICAS={prev_replicas}
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_biography_filters(count, prev_replicas):
    deps = "\n".join(f"    - year_filter_{j}" for j in range(prev_replicas))
    blocks = []
    for i in range(count):
        blocks.append(f"""  biography_filter_{i}:
    build:
      context: ./src/
      dockerfile: biography_filter/Dockerfile
    container_name: biography_filter_{i}
    depends_on:
{deps}
    environment:
    - BATCH_SIZE=100
    - REPLICA_ID={i}
    - INPUT_QUEUE_NAME=article_sections_queue
    - Q2_OUTPUT_QUEUE_NAME=image_queue
    - CONTROL_EXCHANGE_NAME=biography_filter_control
    - CONTROL_ROUTING_KEY=
    - RABBITMQ_HOST=rabbitmq
    - PREVIOUS_STAGE_REPLICAS={prev_replicas}
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_image_downloaders(count, prev_replicas):
    deps = "\n".join(f"    - biography_filter_{j}" for j in range(prev_replicas))
    blocks = []
    for i in range(count):
        blocks.append(f"""  image_downloader_{i}:
    build:
      context: ./src/
      dockerfile: image_downloader/Dockerfile
    container_name: image_downloader_{i}
    depends_on:
{deps}
    environment:
    - BATCH_SIZE=100
    - REPLICA_ID={i}
    - INPUT_QUEUE_NAME=image_queue
    - Q2_OUTPUT_QUEUE_NAME=results_queue
    - CONTROL_EXCHANGE_NAME=image_downloader_control
    - CONTROL_ROUTING_KEY=
    - RABBITMQ_HOST=rabbitmq
    - PREVIOUS_STAGE_REPLICAS={prev_replicas}
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_tokenizers(count):
    blocks = []
    for i in range(count):
        blocks.append(f"""  tokenizer_{i}:
    build:
      context: ./src/
      dockerfile: tokenizer/Dockerfile
    container_name: tokenizer_{i}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
    - BATCH_SIZE=100
    - REPLICA_ID={i}
    - INPUT_QUEUE_NAME=tokenizer_queue
    - Q3_4_OUTPUT_QUEUE_NAME=words_without_stopwords_queue
    - Q5_SUM_OUTPUT_QUEUE_NAME=abstracts_length_queue
    - Q5_AVERAGE_FILTER_OUTPUT_QUEUE_NAME=recent_queue
    - CONTROL_EXCHANGE_NAME=tokenizer_control
    - CONTROL_ROUTING_KEY=
    - RABBITMQ_HOST=rabbitmq
    - PREVIOUS_STAGE_REPLICAS=1
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_unique_words_filters(count, prev_replicas):
    deps = "\n".join(f"    - tokenizer_{j}" for j in range(prev_replicas))
    blocks = []
    for i in range(count):
        blocks.append(f"""  unique_words_filter_{i}:
    build:
      context: ./src/
      dockerfile: unique_words_filter/Dockerfile
    container_name: unique_words_filter_{i}
    depends_on:
{deps}
    environment:
    - BATCH_SIZE=100
    - REPLICA_ID={i}
    - INPUT_QUEUE_NAME=words_without_stopwords_queue
    - Q3_4_OUTPUT_QUEUE_NAME=unique_words_queue
    - CONTROL_EXCHANGE_NAME=unique_words_filter_control
    - CONTROL_ROUTING_KEY=
    - RABBITMQ_HOST=rabbitmq
    - PREVIOUS_STAGE_REPLICAS={prev_replicas}
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_hashers(count, prev_replicas, next_replicas):
    deps = "\n".join(f"    - unique_words_filter_{j}" for j in range(prev_replicas))
    blocks = []
    for i in range(count):
        blocks.append(f"""  hasher_{i}:
    build:
      context: ./src/
      dockerfile: hasher/Dockerfile
    container_name: hasher_{i}
    depends_on:
{deps}
    environment:
    - BATCH_SIZE=100
    - REPLICA_ID={i}
    - INPUT_QUEUE_NAME=unique_words_queue
    - OUTPUT_EXCHANGE_NAME=affinity_words_exchange
    - OUTPUT_ROUTING_KEY_PREFIX=affinity_words
    - CONTROL_EXCHANGE_NAME=hasher_control
    - CONTROL_ROUTING_KEY=
    - RABBITMQ_HOST=rabbitmq
    - PREVIOUS_STAGE_REPLICAS={prev_replicas}
    - NEXT_STAGE_REPLICAS={next_replicas}
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_word_counters(count, prev_replicas, next_replicas):
    deps = "\n".join(f"    - hasher_{j}" for j in range(prev_replicas))
    blocks = []
    for i in range(count):
        blocks.append(f"""  word_counter_{i}:
    build:
      context: ./src/
      dockerfile: word_counter/Dockerfile
    container_name: word_counter_{i}
    depends_on:
{deps}
    environment:
    - BATCH_SIZE=100
    - REPLICA_ID={i}
    - INPUT_EXCHANGE_NAME=affinity_words_exchange
    - INPUT_EXCHANGE_PREFIX=affinity_words
    - RESULTS_QUEUE_NAME=results_queue
    - TOP_EXCHANGE_NAME=client_affinity_exchange
    - TOP_EXCHANGE_RK_PREFIX=client_affinity
    - CONTROL_EXCHANGE_NAME=word_counter_control
    - CONTROL_ROUTING_KEY=
    - RABBITMQ_HOST=rabbitmq
    - PREVIOUS_STAGE_REPLICAS={prev_replicas}
    - NEXT_STAGE_REPLICAS={next_replicas}
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_join_top_20(count, prev_replicas):
    deps = "\n".join(f"    - word_counter_{j}" for j in range(prev_replicas))
    blocks = []
    for i in range(count):
        blocks.append(f"""  join_top_20_{i}:
    build:
      context: ./src/
      dockerfile: join_top_20/Dockerfile
    container_name: join_top_20_{i}
    depends_on:
{deps}
    environment:
    - BATCH_SIZE=100
    - REPLICA_ID={i}
    - INPUT_EXCHANGE_NAME=client_affinity_exchange
    - INPUT_EXCHANGE_PREFIX=client_affinity
    - RESULTS_QUEUE_NAME=results_queue
    - CONTROL_EXCHANGE_NAME=join_top_20_control
    - CONTROL_ROUTING_KEY=
    - RABBITMQ_HOST=rabbitmq
    - PREVIOUS_STAGE_REPLICAS={prev_replicas}
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_sums(count, prev_replicas, sum_join_amount):
    deps_lines = [
        "      rabbitmq:\n        condition: service_healthy"
    ]
    for j in range(sum_join_amount):
        deps_lines.append(f"      sum_join_{j}:\n        condition: service_started")
    deps_block = "\n".join(deps_lines)

    blocks = []
    for i in range(count):
        blocks.append(f"""  sum_{i}:
    build:
      context: ./src/
      dockerfile: sum/Dockerfile
    container_name: sum_{i}
    depends_on:
{deps_block}
    environment:
    - RABBITMQ_HOST=rabbitmq
    - REPLICA_ID={i}
    - PREVIOUS_STAGE_REPLICAS={prev_replicas}
    - INPUT_QUEUE_NAME=abstracts_length_queue
    - CONTROL_EXCHANGE_NAME=sum_control
    - CONTROL_ROUTING_KEY=
    - SUM_JOIN_EXCHANGE=sum_join
    - SUM_JOIN_AMOUNT={sum_join_amount}
    - BATCH_SIZE=100
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_sum_joins(count, prev_replicas, avg_filter_amount):
    deps_lines = [
        "      rabbitmq:\n        condition: service_healthy"
    ]
    for j in range(avg_filter_amount):
        deps_lines.append(f"      average_filter_{j}:\n        condition: service_started")
    deps_block = "\n".join(deps_lines)

    blocks = []
    for i in range(count):
        blocks.append(f"""  sum_join_{i}:
    build:
      context: ./src/
      dockerfile: sum_join/Dockerfile
    container_name: sum_join_{i}
    depends_on:
{deps_block}
    environment:
    - RABBITMQ_HOST=rabbitmq
    - REPLICA_ID={i}
    - PREVIOUS_STAGE_REPLICAS={prev_replicas}
    - SUM_JOIN_EXCHANGE=sum_join
    - AVERAGE_EXCHANGE=average
    - AVERAGE_FILTER_AMOUNT={avg_filter_amount}
    - BATCH_SIZE=10
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_average_filters(count, tokenizer_replicas, sum_join_amount):
    blocks = []
    for i in range(count):
        store_path = f"/tmp/average_filter_store_{i}" if count > 1 else "/tmp/average_filter_store"
        blocks.append(f"""  average_filter_{i}:
    build:
      context: ./src/
      dockerfile: average_filter/Dockerfile
    container_name: average_filter_{i}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
    - RABBITMQ_HOST=rabbitmq
    - BATCH_SIZE=10
    - REPLICA_ID={i}
    - CONTROL_EXCHANGE_NAME=average_filter_control
    - RECENT_QUEUE_NAME=recent_queue
    - AVERAGE_EXCHANGE=average
    - RESULTS_QUEUE_NAME=results_queue
    - TOKENIZER_REPLICAS={tokenizer_replicas}
    - SUM_JOIN_AMOUNT={sum_join_amount}
    - STORE_PATH={store_path}
    - PYTHONUNBUFFERED=1""")
    return "\n\n".join(blocks)


def generate_compose(wanted_queries, replicas, row_batch_size=1000):
    needed_nodes = set()
    for q in wanted_queries:
        needed_nodes.update(QUERY_NODES[q])

    client_depends = ["gateway"]
    for node in NODE_ORDER:
        if node in needed_nodes:
            count = replicas.get(node, 1)
            service_prefix = node.lower()
            for i in range(count):
                client_depends.append(f"{service_prefix}_{i}")

    services = []
    services.append(generate_client(wanted_queries, client_depends, row_batch_size=row_batch_size))
    services.append(generate_gateway(wanted_queries, replicas))
    services.append(generate_rabbitmq())

    if "YEAR_FILTER" in needed_nodes:
        services.append(generate_year_filters(replicas["YEAR_FILTER"]))

    if "LANGUAGE_FILTER" in needed_nodes:
        services.append(generate_language_filters(replicas["LANGUAGE_FILTER"], replicas["YEAR_FILTER"]))

    if "BIOGRAPHY_FILTER" in needed_nodes:
        services.append(generate_biography_filters(replicas["BIOGRAPHY_FILTER"], replicas["YEAR_FILTER"]))

    if "IMAGE_DOWNLOADER" in needed_nodes:
        services.append(generate_image_downloaders(replicas["IMAGE_DOWNLOADER"], replicas["BIOGRAPHY_FILTER"]))

    if "TOKENIZER" in needed_nodes:
        services.append(generate_tokenizers(replicas["TOKENIZER"]))

    if "UNIQUE_WORDS_FILTER" in needed_nodes:
        services.append(generate_unique_words_filters(replicas["UNIQUE_WORDS_FILTER"], replicas["TOKENIZER"]))

    if "HASHER" in needed_nodes:
        services.append(generate_hashers(replicas["HASHER"], replicas["UNIQUE_WORDS_FILTER"], replicas["WORD_COUNTER"]))

    if "WORD_COUNTER" in needed_nodes:
        next_top_replicas = replicas.get("JOIN_TOP_20", 1) if "JOIN_TOP_20" in needed_nodes else 1
        services.append(generate_word_counters(replicas["WORD_COUNTER"], replicas["HASHER"], next_top_replicas))

    if "JOIN_TOP_20" in needed_nodes:
        services.append(generate_join_top_20(replicas["JOIN_TOP_20"], replicas["WORD_COUNTER"]))

    if "SUM" in needed_nodes:
        services.append(generate_sums(replicas["SUM"], replicas["TOKENIZER"], replicas["SUM_JOIN"]))

    if "SUM_JOIN" in needed_nodes:
        services.append(generate_sum_joins(replicas["SUM_JOIN"], replicas["SUM"], replicas["AVERAGE_FILTER"]))

    if "AVERAGE_FILTER" in needed_nodes:
        services.append(generate_average_filters(replicas["AVERAGE_FILTER"], replicas["TOKENIZER"], replicas["SUM_JOIN"]))

    content = "services:\n" + "\n\n".join(services) + "\n"
    return content


def get_wanted_queries_interactive():
    print("=== Selección de Queries ===")
    queries = []
    for i in range(1, 6):
        while True:
            choice = input(f"¿Ejecutar Q{i}? [Y/n]: ").strip().lower()
            if choice in ("y", "yes", "s", "si", ""):
                queries.append(i)
                break
            elif choice in ("n", "no"):
                break
            else:
                print("Por favor, responda 'y' o 'n'.")
    return queries


def get_wanted_nodes_interactive(node, default=1):
    display_name = NODE_DISPLAY_NAMES.get(node, node)
    while True:
        val = input(f"¿Cuántas instancias de {display_name} ({node}) desea ejecutar? [{default}]: ").strip()
        if not val:
            return default
        try:
            n = int(val)
            if n > 0:
                return n
            print("Por favor, ingrese un número mayor a 0.")
        except ValueError:
            print("Por favor, ingrese un número entero válido.")


def parse_replicas_arg(replica_args):
    result = {}
    if not replica_args:
        return result
    for item in replica_args:
        if "=" in item:
            key, val = item.split("=", 1)
            node_key = key.strip().upper()
            try:
                result[node_key] = int(val.strip())
            except ValueError:
                print(f"Error: la cantidad para '{key}' debe ser un entero.")
                sys.exit(1)
        else:
            print(f"Error: formato de réplica inválido '{item}', use NODO=CANTIDAD.")
            sys.exit(1)
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Generador interactivo y CLI de docker-compose.yaml según queries y réplicas deseadas."
    )
    parser.add_argument(
        "-q", "--queries",
        nargs="+",
        type=int,
        choices=[1, 2, 3, 4, 5],
        help="Lista de números de queries a ejecutar (ej: -q 1 2 5)"
    )
    parser.add_argument(
        "-r", "--replicas",
        nargs="+",
        help="Réplicas por nodo en formato NODO=CANTIDAD (ej: -r TOKENIZER=3 HASHER=2 WORD_COUNTER=2)"
    )
    parser.add_argument(
        "-b", "--batch-size",
        type=int,
        default=1000,
        help="ROW_BATCH_SIZE del cliente (default: 1000)"
    )
    parser.add_argument(
        "-o", "--output",
        default="docker-compose.yaml",
        help="Ruta del archivo docker-compose.yaml de salida (default: docker-compose.yaml)"
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Aceptar valores por defecto (1 réplica) para nodos no especificados sin preguntar."
    )

    args = parser.parse_args()

    # Selección de queries
    if args.queries is not None:
        wanted_queries = sorted(list(set(args.queries)))
    else:
        wanted_queries = get_wanted_queries_interactive()

    if not wanted_queries:
        print("Error: No se seleccionó ninguna query. Terminando.")
        sys.exit(1)

    print(f"\nQueries seleccionadas: {[f'Q{q}' for q in wanted_queries]}")

    # Determinar qué nodos son necesarios
    needed_nodes = set()
    for q in wanted_queries:
        needed_nodes.update(QUERY_NODES[q])

    # Parsear réplicas pasadas por CLI
    cli_replicas = parse_replicas_arg(args.replicas)

    # Replicas finales
    replicas = {}
    is_interactive = sys.stdin.isatty() and not args.yes
    print("\n=== Configuración de Réplicas ===")
    for node in NODE_ORDER:
        if node in needed_nodes:
            if node in cli_replicas:
                replicas[node] = cli_replicas[node]
                print(f"{NODE_DISPLAY_NAMES[node]} ({node}): {replicas[node]} instancias (especificado por CLI)")
            elif is_interactive:
                default_count = 1
                replicas[node] = get_wanted_nodes_interactive(node, default=default_count)
            else:
                replicas[node] = 1
                print(f"{NODE_DISPLAY_NAMES[node]} ({node}): 1 instancia (default)")

    # Generar contenido
    compose_content = generate_compose(wanted_queries, replicas, row_batch_size=args.batch_size)

    # Determinar ruta destino
    output_path = args.output
    # Si no se pasó directorio absoluto y output es "docker-compose.yaml", verificar si estamos en repo root o en python/
    if output_path == "docker-compose.yaml" and os.path.isdir("python") and not os.path.exists("src"):
        output_path = os.path.join("python", "docker-compose.yaml")

    with open(output_path, "w") as f:
        f.write(compose_content)

    print(f"\n¡Éxito! Archivo docker-compose generado correctamente en: {output_path}")
    print("\nResumen de la configuración generada:")
    print(f"  Queries: {', '.join(f'Q{q}' for q in wanted_queries)}")
    print("  Nodos y réplicas:")
    for node in NODE_ORDER:
        if node in needed_nodes:
            print(f"    - {NODE_DISPLAY_NAMES[node]} ({node}): {replicas[node]}")


if __name__ == "__main__":
    main()
