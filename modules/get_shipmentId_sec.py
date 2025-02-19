import os
import requests
import pandas as pd
import datetime
import re
from .db_connection import Db_Connection
from dotenv import load_dotenv

class IdSecundaria:
    def __init__(self):
        load_dotenv()
        self._authtoken = os.getenv("AUTHTOKEN")
        self._start_date = datetime.datetime.today().date() - datetime.timedelta(days=3)
        self._end_date = datetime.datetime.today().date() + datetime.timedelta(days=1)
        self._url = "https://gw.jtjms-br.com/transportation/tmsBranchTrackingDetail/page"
        self._headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6,zh-CN;q=0.5,zh;q=0.4",
            "Cache-Control": "max-age=2, must-revalidate",
            "Connection": "keep-alive",
            "Content-Length": "163",
            "Content-Type": "application/json;charset=UTF-8",
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
            "routeName": "brancTaskTrackSearch",
            "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "timezone": "GMT-0300"
        }


    def get_shipment_no(self):

        found_shipmentId = []

        params = {
            "current":1,
            "size":300,
            "startDepartureTime": f"{self._start_date} 00:00:00",
            "endDepartureTime": f"{self._end_date} 23:59:59",
            "distributionType":2,
            "countryId":"1"
            }

        shipmentNos = requests.post(url = self._url, headers = self._headers, json = params, ).json()["data"]["records"]

        for shipmentNo in shipmentNos:
            if shipmentNo["shipmentState"] in [0,2,3,4] and shipmentNo["endName"] in ["CD VAG 001", "DC RAO 001", "CD UBA 001", "CD EXT 001"]:

                img_header = {
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
                    "routeName": "brancTaskTrackSearchView",
                    "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "timezone": "GMT-0300"
                }

                try:
                    shipment_imgs = requests.get(url = f"https://gw.jtjms-br.com/transportation/tmsBranchTrackingDetail/locking/scan/page?shipmentNo={shipmentNo['shipmentNo']}", headers = img_header).json()["data"]["records"][-1]
                except:
                    shipment_imgs = {}


                shipment_states = [
                    "Planejado",
                    "Programado",
                    "Carregando",
                    "Em trânsito",
                    "Concluído",
                    "Cancelado"
                ]

                today = datetime.datetime.today().date()
                tomorrow = today + datetime.timedelta(days=1)
                tomorrow2 = today + datetime.timedelta(days=2)
                tomorrow3 = today + datetime.timedelta(days=3)

                planned_date = datetime.datetime.strptime(shipmentNo["plannedDepartureTime"], "%Y-%m-%d %H:%M:%S").date()

                # Verifica se deve ser adicionado à lista
                if shipmentNo["endName"] == "CD EXT 001" and planned_date > today or shipmentNo["endName"] != "CD EXT 001":
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
                        shipment_states[shipmentNo["shipmentState"]],
                        shipmentNo["plannedDepartureTime"],
                        shipmentNo["plannedArrivalTime"],
                        shipmentNo["actualDepartureTime"] if shipmentNo["actualDepartureTime"] is not None else shipmentNo["plannedDepartureTime"],
                        shipmentNo["actualArrivalTime"] if shipmentNo["actualArrivalTime"] is not None else shipmentNo["plannedArrivalTime"],
                        shipmentNo["scanMainWaybillNum"],
                        shipment_imgs.get("lockTimeImg", ""),
                        shipment_imgs.get("packSendImg", ""),
                        shipment_imgs.get("mdfePicImg", "")
                    ])

        return found_shipmentId

    def main(self, create_wb = False):

        connection = Db_Connection()
        cursor = connection.get_cursor()
        conn = connection.get_conn()


        dados_ids = IdSecundaria().get_shipment_no()
        print("Coletando ID de viagem...")

        # # Inserção dos dados no DB
        # clear_table = "DELETE FROM public.shipment_nos_sec;"
        # cursor.execute(clear_table)
        # print("Limpando tabela antiga...")

        # query = """
        # INSERT INTO public.shipment_nos_sec (
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