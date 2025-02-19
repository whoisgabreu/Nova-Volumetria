from modules.get_shipmentId import IdTroncal
from modules.get_scanned_amount import CarregamentoTroncal
from modules.get_shipmentId_sec import IdSecundaria
from modules.get_scanned_amount_sec import CarregamentoSecundaria
from modules.get_token import Get_Token

# Módulo de login automático para coletar Authtoken
# Editar o DotEnv incluindo o novo token

def update_dotenv(token):
    import re
    import os

    # Caminho do arquivo .env
    env_file = os.path.join(os.path.dirname(__file__), ".env")

    # Novo valor do AUTHTOKEN
    novo_authtoken = token

    # Lendo o arquivo e substituindo o valor de AUTHTOKEN
    with open(env_file, "r", encoding="utf-8") as file:
        conteudo = file.read()

    # Substituir o valor do AUTHTOKEN
    conteudo_atualizado = re.sub(r'(AUTHTOKEN\s*=\s*)".*?"', rf'\1"{novo_authtoken}"', conteudo)

    # Escrever o novo conteúdo de volta no arquivo
    with open(env_file, "w", encoding="utf-8") as file:
        file.write(conteudo_atualizado)

    print("AUTHTOKEN atualizado com sucesso!")

authtoken = Get_Token().start()
if authtoken:
    update_dotenv(authtoken)

    IdTroncal().main()
    IdSecundaria().main()

    CarregamentoTroncal().main()
    CarregamentoSecundaria().main()