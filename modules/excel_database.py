import pandas as pd
import datetime
from db_connection import Db_Connection

today = datetime.datetime.today().date()

cursor = Db_Connection().get_cursor()

# Volumetria
#  WHERE planned_arrival_time > '{today} 00:00:00'
cursor.execute("""
SELECT * FROM public.scanned_amount
""")

volumetria = cursor.fetchall()
df_header = ["id","billcode", "network_name", "network_code", "next_station", "package_code", "scan_time", "scan_type",
            "scan_user", "total", "dest_site", "dest_site_city", "deliveryOutNetworkCode", "deliveryOutNetworkName",
            "outNetworkCode", "outNetworkName", "shipment_No", "planned_arrival_Time", "actual_arrival_time", "final_estimated_arrival"]

df = pd.DataFrame(volumetria)
df.to_excel("Teste Volumetria/scanned_amount.xlsx", index = False, header = df_header)

# ID Viagem Tronco
#  WHERE planned_arrival_time > '{today} 00:00:00'
cursor.execute(f"""
SELECT * FROM public.shipment_nos
""")

shipment_nos = cursor.fetchall()
df_header_2 = ["id", "shipment_no", "shipment_name", "vehicle_typegroup", "carrier_name", "driver_name", "plate_number", "driver_name_two", "plate_number_two", "start_name", "end_name", "shipment_state", "planned_departure_time", "actual_departure_time", "planned_arrival_time", "actual_arrival_time", "scan_mainwaybillnum", "lock_time_img", "pack_send_img", "mdfe_pic_img"]

df = pd.DataFrame(shipment_nos)
df.to_excel("Teste Volumetria/shipment_nos.xlsx", index = False, header = df_header_2)

# ID Viagem Secundária
#  WHERE planned_arrival_time > '{today} 00:00:00' 
cursor.execute("""
SELECT * FROM public.shipment_nos_sec
""")

shipment_nos_sec = cursor.fetchall()
df_header_3 = ["id", "shipment_no", "shipment_name", "vehicle_typegroup", "carrier_name", "driver_name", "plate_number", "driver_name_two", "plate_number_two", "start_name", "end_name", "shipment_state", "planned_departure_time", "actual_departure_time", "planned_arrival_time", "actual_arrival_time", "scan_mainwaybillnum", "lock_time_img", "pack_send_img", "mdfe_pic_img"]

df = pd.DataFrame(shipment_nos_sec)
df.to_excel("Teste Volumetria/shipment_nos_sec.xlsx", index = False, header = df_header_3)