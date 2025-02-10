import os
import json
import matplotlib.pyplot as plt
import numpy as np


def convert_numpy(obj):
    """ Converte objetos numpy para tipos Python compatíveis com JSON """
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def get_safe_int(value):
    """ Garante que o valor seja um int, se aplicável """
    return int(value) if isinstance(value, (int, np.integer)) else value


def generate_filename(prefix, result, start_date, end_date):
    """ Gera o nome do arquivo de forma centralizada """
    return f'results/{prefix}_profit{get_safe_int(result["profit"])}_loss{get_safe_int(result["loss"])}_div{get_safe_int(result["diversification"])}_short{get_safe_int(result["window"][0])}_long{get_safe_int(result["window"][1])}_{start_date}_to_{end_date}.json'


def save_json(filename, data):
    """ Salva um dicionário como JSON """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, default=convert_numpy)


def generate_performance_plot(directory: str = "results", output_prefix: str = "performance_comparison"):
    """
    Gera um gráfico contendo todas as linhas das simulações a partir dos arquivos JSON na pasta `directory`.

    :param directory: Pasta onde os arquivos JSON estão localizados.
    :param output_prefix: Prefixo para o nome do arquivo de saída do gráfico.
    """

    plt.figure(figsize=(10, 6))

    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            try:
                with open(os.path.join(directory, filename), "r") as timeline_file:
                    timeline = json.load(timeline_file)

                    # Extrair parâmetros do nome do arquivo
                    params = filename.replace(
                        "timeline_", "").replace(".json", "")
                    labels = params.split("_")
                    profit = labels[0].replace("profit", "")
                    loss = labels[1].replace("loss", "")
                    div = labels[2].replace("div", "")
                    short = labels[3].replace("short", "")
                    long = labels[4].replace("long", "")

                    allocation_over_time = []
                    for entry in timeline:
                        allocation = sum(
                            item['quantidade'] * item['preco_compra'] for item in entry['portfolio']
                        )
                        allocation_over_time.append(allocation)

                    interval = range(len(allocation_over_time))

                    plt.plot(interval[2:], allocation_over_time[2:], label=f"Profit={profit}, Loss={loss}, "
                             f"Div={div}, Short={short}, Long={long}")

            except FileNotFoundError:
                print(f"Arquivo não encontrado: {filename}")
                continue

    plt.title("Alocação em Ativos por Configuração")
    plt.xlabel("Período")
    plt.ylabel("Valor (R$)")
    plt.legend(loc='upper left', fontsize=8)
    plt.grid(True)
    plt.tight_layout()

    plt.savefig(f"results/{output_prefix}.png", format="png")
    plt.show()

    plt.close()


# generate_performance_plot()
