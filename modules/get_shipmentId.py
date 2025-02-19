import os
import requests
import pandas as pd
import datetime
import re
from .db_connection import Db_Connection
from dotenv import load_dotenv

class IdTroncal:
    def __init__(self):
        load_dotenv()
        self._authtoken = os.getenv("AUTHTOKEN")
        self._start_date = datetime.datetime.today().date() - datetime.timedelta(days = 3)
        self._end_date = datetime.datetime.today().date()
        self._url = "https://gw.jtjms-br.com/transportation/tmsShipment/page"
        self._headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6,zh-CN;q=0.5,zh;q=0.4",
            "Cache-Control": "max-age=2, must-revalidate",
            "Connection": "keep-alive",
            "Content-Type": "application/json;charset=utf-8",
            "Host": "gw.jtjms-br.com",
            "Origin": "https://jmsbr.jtjms-br.com",
            "Referer": "https://jmsbr.jtjms-br.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "authToken": self._authtoken,
            "lang": "PT",
            "langType": "PT",
            "routeName": "monitoringSearch",
            "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "timezone": "GMT-0300"
        }


    # Lista de siglas que você deseja verificar

    # Função para verificar
    def verificar_sigla(self, string):
        siglas = ["CGE", "BSB", "SJM"]

        # Expressão regular
        regex = r"^BRE-(?:" + "|".join(siglas) + r")\b"

        return bool(re.match(regex, string))

    def get_shipment_no(self):

        found_shipmentId = []

        params = {
            "current": 1,
            "size": 300,
            "startDateTime": f"{self._start_date} 22:00:00",
            "endDateTime": f"{self._end_date} 23:59:59",
            "searchType": "manage"
        }

        shipmentNos = requests.get(url = self._url, params = params, headers = self._headers).json()["data"]["records"]


        for shipmentNo in shipmentNos:
            #  and self.verificar_sigla(shipmentNo["shipmentName"]) 
            if shipmentNo["shipmentState"] in [3,4]and shipmentNo["endName"] in ["SC CGE 02", "SC SJM 01", "SC BSB 01", "DC SRR 001"]:

                shipment_imgs = requests.get(url = f"https://gw.jtjms-br.com/transportation/trackingDeatil/locking/scan/page?current=1&size=50&shipmentNo={shipmentNo['shipmentNo']}", headers = self._headers).json()["data"]["records"][-1]


                found_shipmentId.append([
                    shipmentNo["shipmentNo"],
                    shipmentNo["shipmentName"],
                    shipmentNo["vehicleTypegroup"],
                    shipmentNo["carrierName"],
                    shipmentNo["driverName"],
                    shipmentNo["plateNumber"],
                    shipmentNo["driverNameTwo"],
                    shipmentNo["plateNumberTwo"],
                    shipmentNo["startName"],
                    shipmentNo["endName"],
                    "Em Trânsito" if shipmentNo["shipmentState"] == 3 else "Concluído",
                    shipmentNo["plannedDepartureTime"],
                    shipmentNo["plannedArrivalTime"],
                    shipmentNo["actualDepartureTime"] if shipmentNo["actualDepartureTime"] != None else shipmentNo["plannedDepartureTime"],
                    shipmentNo["actualArrivalTime"] if shipmentNo["actualArrivalTime"] != None else shipmentNo["plannedArrivalTime"],
                    shipmentNo["scanMainWaybillNum"],
                    shipment_imgs["lockTimeImg"],
                    shipment_imgs["packSendImg"],
                    shipment_imgs["mdfePicImg"]
                ])

        return found_shipmentId

    def main(self, create_wb = False):

        connection = Db_Connection()
        cursor = connection.get_cursor()
        conn = connection.get_conn()


        dados_ids = IdTroncal().get_shipment_no()
        print("Coletando ID de viagem...")

        # # Inserção dos dados no DB
        # clear_table = "DELETE FROM public.shipment_nos;"
        # cursor.execute(clear_table)
        # print("Limpando tabela antiga...")

        # query = """
        # INSERT INTO public.shipment_nos (
        #     shipment_no, shipment_name, vehicle_typegroup, start_name, end_name, shipment_state, planned_departure_time,  planned_arrival_time, actual_departure_time,actual_arrival_time, scan_mainwaybillnum, lock_time_img, pack_send_img, mdfe_pic_img
        #     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        # """

        query = """
        INSERT INTO public.shipment_nos (
            shipment_no, shipment_name, vehicle_typegroup, carrier_name, driver_name, plate_number, driver_name_two, plate_number_two, start_name, end_name, shipment_state, 
            planned_departure_time, planned_arrival_time, actual_departure_time, actual_arrival_time, 
            scan_mainwaybillnum, lock_time_img, pack_send_img, mdfe_pic_img
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (shipment_no) 
        DO UPDATE SET 
            shipment_state = EXCLUDED.shipment_state,
            actual_departure_time = EXCLUDED.actual_departure_time,
            actual_arrival_time = EXCLUDED.actual_arrival_time,
            scan_mainwaybillnum = EXCLUDED.scan_mainwaybillnum,
            lock_time_img = EXCLUDED.lock_time_img,
            pack_send_img = EXCLUDED.pack_send_img,
            mdfe_pic_img = EXCLUDED.mdfe_pic_img;
        """

        cursor.executemany(query, dados_ids)

        # Corrigir erro de Caracter
        update_table = """
        UPDATE public.shipment_nos 
        SET carrier_name = REPLACE(carrier_name, '&amp;', '&')
        WHERE carrier_name LIKE '%&amp;%';
        """
        cursor.execute(update_table)

        conn.commit()
        cursor.close()
        conn.close()
        print("Dados inseridos na tabela 'public.shipment_nos'!")