from modules.get_shipmentId import IdTroncal
from modules.get_scanned_amount import CarregamentoTroncal
from modules.get_shipmentId_sec import IdSecundaria
from modules.get_scanned_amount_sec import CarregamentoSecundaria
from modules.get_token import Get_Token

import schedule
import time

# Módulo de login automático para coletar Authtoken
# Editar o DotEnv incluindo o novo token

def update_dotenv(token):
    import re
    import os

    env_file = os.path.join(os.path.dirname(__file__), ".env")

    novo_authtoken = token

    with open(env_file, "r", encoding="utf-8") as file:
        conteudo = file.read()

    conteudo_atualizado = re.sub(r'(AUTHTOKEN\s*=\s*)".*?"', rf'\1"{novo_authtoken}"', conteudo)

    with open(env_file, "w", encoding="utf-8") as file:
        file.write(conteudo_atualizado)

    print("AUTHTOKEN atualizado com sucesso!")


def main_func():
    authtoken = Get_Token().start()
    if authtoken:
        update_dotenv(authtoken)

        IdTroncal().main()
        IdSecundaria().main()

        CarregamentoTroncal().main()
        CarregamentoSecundaria().main()


schedule.every().day.at("08:00").do(main_func)
schedule.every().day.at("12:00").do(main_func)
schedule.every().day.at("15:00").do(main_func)
schedule.every().day.at("18:00").do(main_func)
schedule.every().day.at("21:00").do(main_func)

while True:
    schedule.run_pending()
    time.sleep(30)