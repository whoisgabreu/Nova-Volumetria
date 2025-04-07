# import pandas as pd
# import os
# import datetime
# from .db_connection import Db_Connection

# def gerar_xlsx():

#     excel_dir = os.path.join(os.path.expanduser('~'), 'OneDrive', 'OneDrive - J&T EXPRESS BRAZIL LTDA', 'PowerBi Volumetria SC')

#     today = datetime.datetime.today().date()
#     cursor = Db_Connection().get_cursor()

#     # Volumetria
#     #  WHERE planned_arrival_time > '{today} 00:00:00'
#     cursor.execute("""
#     SELECT
#         *
#     FROM
#         SCANNED_AMOUNT
#     WHERE
#         FINAL_ESTIMATED_ARRIVAL >= CURRENT_DATE - INTERVAL '3 days'
#         AND UF = 'MG'
#     ORDER BY
#         FINAL_ESTIMATED_ARRIVAL ASC
#     """)


#     volumetria = cursor.fetchall()
#     df_header = ["id","billcode", "network_name", "network_code", "next_station", "package_code", "scan_time", "scan_type",
#                 "scan_user", "total", "dest_site", "dest_site_city", "deliveryOutNetworkCode", "deliveryOutNetworkName",
#                 "outNetworkCode", "outNetworkName", "shipment_No", "planned_arrival_Time", "actual_arrival_time", "final_estimated_arrival", 'uf']

#     df = pd.DataFrame(volumetria)
#     df.to_excel(f"{excel_dir}/scanned_amount.xlsx", index = False, header = df_header)

#     # ID Viagem Tronco
#     #  WHERE planned_arrival_time > '{today} 00:00:00'
#     cursor.execute(f"""
#     -- Filtrar ambos ID's
# SELECT
# 	SHIPMENT_NO,
# 	SHIPMENT_STATE,
# 	ACTUAL_DEPARTURE_TIME
# FROM
# 	PUBLIC.SHIPMENT_NOS
# WHERE
# 	(
# 		ACTUAL_DEPARTURE_TIME >= CURRENT_DATE - INTERVAL '1 DAY' + INTERVAL '3 HOURS'
# 		AND ACTUAL_DEPARTURE_TIME <= CURRENT_TIMESTAMP + INTERVAL '1 DAY'
# 		AND SHIPMENT_NO LIKE 'DK%'
# 	)
# 	OR (
# 		ACTUAL_DEPARTURE_TIME > CURRENT_DATE + INTERVAL '3 hours'
# 		AND ACTUAL_DEPARTURE_TIME <= CURRENT_TIMESTAMP + INTERVAL '1 DAY'
# 		AND SHIPMENT_NO LIKE 'SR%'
# 	)

# ORDER BY
# 	ACTUAL_DEPARTURE_TIME ASC;
#     """)

#     shipment_nos = cursor.fetchall()
#     # df_header_2 = ["id", "shipment_no", "shipment_name", "vehicle_typegroup", "carrier_name", "driver_name", "plate_number", "driver_name_two", "plate_number_two", "start_name", "end_name", "shipment_state", "planned_departure_time", "actual_departure_time", "planned_arrival_time", "actual_arrival_time", "scan_mainwaybillnum", "lock_time_img", "pack_send_img", "mdfe_pic_img"]
#     df_header_2 = ["shipment_no", "shipment_state", "actual_departure_time"]

#     df = pd.DataFrame(shipment_nos)
#     df.to_excel(f"{excel_dir}/shipment_nos.xlsx", index = False, header = df_header_2)


# ------------------------------------------------

import pandas as pd
import os
import datetime
from .db_connection import Db_Connection

def compare_shipment_name():
    excel_dir = os.path.join(os.path.expanduser('~'), 'OneDrive - J&T EXPRESS BRAZIL LTDA', 'PowerBi Volumetria SC')
    file_path = os.path.join(excel_dir, "data_nome_rota.xlsx")

    # Lendo apenas as colunas necessárias
    df = pd.read_excel(file_path, usecols=["dest_site", "shipment_name", "Trecho-Base"])
    
    return df

def gerar_xlsx():

    excel_dir = os.path.join(os.path.expanduser('~'), 'OneDrive - J&T EXPRESS BRAZIL LTDA', 'PowerBi Volumetria SC')

    today = datetime.datetime.today().date()
    cursor = Db_Connection().get_cursor()

    # Volumetria
    cursor.execute("""
    SELECT
        *
    FROM
        SCANNED_AMOUNT
    WHERE
        FINAL_ESTIMATED_ARRIVAL >= CURRENT_DATE - INTERVAL '3 days'
        AND UF = 'MG'
    ORDER BY
        FINAL_ESTIMATED_ARRIVAL ASC
    """)

    volumetria = cursor.fetchall()
    df_header = ["id","billcode", "network_name", "network_code", "next_station", "package_code", "scan_time", "scan_type",
                "scan_user", "total", "dest_site", "dest_site_city", "deliveryOutNetworkCode", "deliveryOutNetworkName",
                "outNetworkCode", "outNetworkName", "shipment_no", "planned_arrival_Time", "actual_arrival_time", "final_estimated_arrival", 'uf']

    df1 = pd.DataFrame(volumetria, columns=df_header)

    # ID Viagem Tronco
    cursor.execute(f"""
    SELECT
        SHIPMENT_NO,
        SHIPMENT_NAME,  -- Adicionando a coluna shipment_name aqui
        SHIPMENT_STATE,
        ACTUAL_DEPARTURE_TIME
    FROM
        PUBLIC.SHIPMENT_NOS
    WHERE
        (
            ACTUAL_DEPARTURE_TIME >= CURRENT_DATE - INTERVAL '1 DAY' + INTERVAL '3 HOURS'
            AND ACTUAL_DEPARTURE_TIME <= CURRENT_TIMESTAMP + INTERVAL '1 DAY'
            AND SHIPMENT_NO LIKE 'DK%'
        )
        OR (
            ACTUAL_DEPARTURE_TIME > CURRENT_DATE + INTERVAL '3 hours'
            AND ACTUAL_DEPARTURE_TIME <= CURRENT_TIMESTAMP + INTERVAL '1 DAY'
            AND SHIPMENT_NO LIKE 'SR%'
        )
    ORDER BY
        ACTUAL_DEPARTURE_TIME ASC;
    """)

    shipment_nos = cursor.fetchall()
    df_header_2 = ["shipment_no", "shipment_name", "shipment_state", "actual_departure_time"]
    
    df2 = pd.DataFrame(shipment_nos, columns=df_header_2)

    df3 = compare_shipment_name()

    # Fazer o merge para adicionar shipment_name ao df1
    df1 = df1.merge(df3[['dest_site', 'shipment_name']], on='dest_site', how='left')
    df1 = df1.merge(df3[['dest_site', 'Trecho-Base']], on='dest_site', how='left')

    # Salvar os arquivos Excel
    df1.to_excel(f"{excel_dir}/scanned_amount.xlsx", index=False)
    df2.to_excel(f"{excel_dir}/shipment_nos.xlsx", index=False)


# gerar_xlsx()