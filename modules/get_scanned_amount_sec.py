import os
import requests
import pandas as pd
import datetime
from .db_connection import Db_Connection
from dotenv import load_dotenv

class CarregamentoSecundaria:
    def __init__(self):
        load_dotenv(override = True)
        self._authtoken = os.getenv("AUTHTOKEN")
        self._url = "https://gw.jtjms-br.com/transportation/tmsBranchTrackingDetail/loading/scan/page"
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
            "routeName": "brancTaskTrackSearchLoading",
            "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "timezone": "GMT-0300"
        }

    def get_scanned_amount(self, shipmentNo, planned_arrival_time, actual_arrival_time):
        
        bases_dict = {
            "CD UDI 001": "MG",
            "CD ITU 001": "MG",
            "CD FRT 001": "MG",
            "CD IRM 001": "MG",
            "CD AGI 001": "MG",
            "CD GHT 001": "MG",
            "CD CTR 001": "MG",
            "CD IMS 001": "MG",
            "CD TPC 001": "MG",
            "CD LJA 001": "MG",
            "CD CTM 001": "MG",
            "CD FCS 001": "MG",
            "CD CRC 001": "MG",
            "CD SBD 001": "MG",
            "CD MAG 001": "MG",
            "OLD-CD NVL 01": "MG",
            "CD DIQ 001": "MG",
            "CD FMG 001": "MG",
            "CD NSR 001": "MG",
            "CD OLV 001": "MG",
            "CD AET 001": "MG",
            "CD LGP 001": "MG",
            "CD IPC 001": "MG",
            "CD PAEXT 001": "MG",
            "CD BCV 001": "MG",
            "CD PPR 001": "MG",
            "CD JNA 001": "MG",
            "CD JUB 001": "MG",
            "CD MTC 001": "MG",
            "CD CUV 001": "MG",
            "CD DMA 001": "MG",
            "CD BMN 001": "MG",
            "CD VZP 001": "MG",
            "CD JIB 001": "MG",
            "CD UBI 001": "MG",
            "CD MVE 001": "MG",
            "CD VCS 001": "MG",
            "CD UAB 001": "MG",
            "CD PTN 001": "MG",
            "CD FVR 001": "MG",
            "CD PTF 001": "MG",
            "CD ERV 001": "MG",
            "CD VAG 001": "MG",
            "CD AFS 001": "MG",
            "CD PSS 001": "MG",
            "CD LAS 001": "MG",
            "CD UNI 001": "MG",
            "CD PTU 001": "MG",
            "CD POJ 001": "MG",
            "CD PTC 001": "MG",
            "CD UBA 001": "MG",
            "CD AAX 001": "MG",
            "CD CDA 001": "MG",
            "CD UBA 002": "MG",
            "CD ITA 001": "MG",
            "CD CPA 001": "MG",
            "CD AMN 001": "MG",
            "CD AUI 001": "MG",
            "CD SLN 001": "MG",
            "CD AGF 001": "MG",
            "CD TFL 001": "MG",
            "CD EXT 001": "MG",
            "CD PPY 001": "MG",
            "CD POO 001": "MG",
            "CD OUF 001": "MG",
            "CD BHZ 001": "MG",
            "CD RDN 001": "MG",
            "CD STL 001": "MG",
            "CD BHZ 005": "MG",
            "CD VPS 001": "MG",
            "CD STZ 001": "MG",
            "CD SAB 001": "MG",
            "CD RDN 002": "MG",
            "CD BHZ 003": "MG",
            "CD MNH 001": "MG",
            "CD CTG 001": "MG",
            "CD CRL 001": "MG",
            "CD IPA 001": "MG",
            "CD JDF 001": "MG",
            "CD CGS 001": "MG",
            "CD ALP 001": "MG",
            "CD MRE 001": "MG",
            "CD GVR 001": "MG",
            "CD CSP 001": "MG",
            "CD MTA 001": "MG",
            "CD AMS 001": "MG",
            "CD PNH 001": "MG",
            "CD BHZ 004": "MG",
            "CD CGE 001": "MG",
            "CD JTB 001": "MG",
            "CD PABTM 001": "MG",
            "CD BTM 002": "MG",
            "CD NVL 001": "MG",
            "CD PABHZ 001": "MG",
            "CD BHZ 002": "MG",
            "CD BTM 001": "MG",
            "CD SJD 001": "MG",
            "CD OPT 001": "MG",
            "CD BCA 001": "MG",
            "CD QDF 001": "MG",
            "CD JMV 001": "MG",
            "CD GHE 001": "MG",
            "CD TTO 001": "MG",
            "CD NAQ 001": "MG",
            "CD IBR 001": "MG",
            "CD IPN 001": "MG"
        }

        params = {
            "current": 1,
            "size": 30000,
            "shipmentNo": shipmentNo,
            "scanNetworkCode": 31101,
        }

        target_time1 = datetime.datetime.combine(
            datetime.date.today(),  # Obtém a data de hoje
            datetime.time(12, 10)   # Define o horário desejado (12:10)
        )

        target_time2 = datetime.datetime.combine(
            datetime.date.today() + datetime.timedelta(days = 1),  # Obtém a data de hoje
            datetime.time(6, 0)   # Define o horário desejado (12:10)
        )

        scanned_waybillNo = requests.get(url = self._url, params = params, headers = self._headers).json()["data"]["records"]
        for record in scanned_waybillNo:
            record["shipmentNo"] = shipmentNo
            record["plannedArrivalTime"] = planned_arrival_time
            record["actualArrivalTime"] = actual_arrival_time

            if actual_arrival_time >= target_time1 and actual_arrival_time <= target_time2:
                record["finalEstimatedArrival"] = datetime.date.today() + datetime.timedelta(days = 1)

            else:
                record["finalEstimatedArrival"] = actual_arrival_time.date()

            record["uf"] = bases_dict.get(record["destsite"], "Não é MG")

        return scanned_waybillNo


    def main(self):

        all_scanned_waybill_no = []

        connection = Db_Connection()
        cursor = connection.get_cursor()
        conn = connection.get_conn()

        query = """
        INSERT INTO public.scanned_amount (
           billcode,  network_name, network_code, next_station, package_code, scan_time, scan_type, scan_user, total, dest_site, dest_site_city, deliveryOutNetworkCode, deliveryOutNetworkName, outNetworkCode, outNetworkName, shipment_No, planned_arrival_Time, actual_arrival_time, final_estimated_arrival, uf
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (billcode)
        DO UPDATE SET 
            actual_arrival_time = EXCLUDED.actual_arrival_time;
        """
        cursor.execute("""
        SELECT shipment_no, planned_arrival_time, actual_arrival_time from public.shipment_nos WHERE actual_arrival_time >= CURRENT_TIMESTAMP - INTERVAL '3 days'
        """)

        shipments = cursor.fetchall()
        print("\nColetando ID de viagem da tabela 'public.shipment_nos'...\n")

        for shipment in shipments:
            all_scanned_waybill_no += self.get_scanned_amount(shipmentNo = shipment[0], planned_arrival_time = shipment[1], actual_arrival_time = shipment[2])
        print("Coletando dados de carregamento...\n")

        # Supondo que 'all_scanned_waybill_no' seja sua lista de dicionários com os dados
        df = pd.DataFrame(all_scanned_waybill_no)

        df["plannedArrivalTime"] = df["plannedArrivalTime"].where(pd.notna(df["plannedArrivalTime"]), None)
        df["actualArrivalTime"] = df["actualArrivalTime"].where(pd.notna(df["actualArrivalTime"]), None)

        data_tuples = list(df.itertuples(index=False, name=None))

        cursor.executemany(query, data_tuples)
        conn.commit()
        cursor.close()
        conn.close()
        print("Dados de Carregamento inseridos na tabela 'public.scanned_amount'!\n")