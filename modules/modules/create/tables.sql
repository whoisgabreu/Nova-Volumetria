DROP TABLE IF EXISTS shipment_nos;
CREATE TABLE shipment_nos (
    id SERIAL PRIMARY KEY,
    shipment_No VARCHAR(100) UNIQUE NOT NULL,
    shipment_Name VARCHAR(100) NOT NULL,
    vehicle_Typegroup VARCHAR(255),
    carrier_name VARCHAR(255),
    driver_name VARCHAR(255),
    plate_number VARCHAR(255),
    driver_name_two VARCHAR(255),
    plate_number_two VARCHAR(255),
    start_Name VARCHAR(15) NOT NULL,
    end_Name VARCHAR(15) NOT NULL,
	shipment_state VARCHAR(30) NOT NULL,
    planned_departure_Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    planned_arrival_Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    actual_departure_Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    actual_arrival_Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scan_MainWaybillNum INT,
    lock_time_img VARCHAR(300),
    pack_send_img VARCHAR(300),
    mdfe_pic_img VARCHAR(300)
);

DROP TABLE IF EXISTS shipment_nos_sec;
CREATE TABLE shipment_nos_sec (
    id SERIAL PRIMARY KEY,
    shipment_No VARCHAR(100) UNIQUE NOT NULL,
    shipment_Name VARCHAR(100) NOT NULL,
    vehicle_Typegroup VARCHAR(255),
    carrier_name VARCHAR(255),
    driver_name VARCHAR(255),
    plate_number VARCHAR(255),
    driver_name_two VARCHAR(255),
    plate_number_two VARCHAR(255),
    start_Name VARCHAR(15) NOT NULL,
    end_Name VARCHAR(15) NOT NULL,
	shipment_state VARCHAR(30) NOT NULL,
    planned_departure_Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    planned_arrival_Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    actual_departure_Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    actual_arrival_Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scan_MainWaybillNum INT,
    lock_time_img VARCHAR(300),
    pack_send_img VARCHAR(300),
    mdfe_pic_img VARCHAR(300)
);

DROP TABLE IF EXISTS scanned_amount;
CREATE TABLE scanned_amount (
    id SERIAL PRIMARY KEY,
    billcode VARCHAR(100) UNIQUE,
    network_name VARCHAR(100),
    network_code VARCHAR(100),
    next_station VARCHAR(100),
    package_code VARCHAR(100),
    scan_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scan_type VARCHAR(1),
    scan_user varchar(100),
    total INT,
    dest_site VARCHAR(100),
    dest_site_city VARCHAR(100),
    deliveryOutNetworkCode VARCHAR(100),
    deliveryOutNetworkName VARCHAR(100),
    outNetworkCode VARCHAR(100),
    outNetworkName VARCHAR(100),
    shipment_No VARCHAR(100) NOT NULL,
    planned_arrival_Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    actual_arrival_Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    final_estimated_arrival DATE DEFAULT CURRENT_DATE,
	uf VARCHAR(100)
);













-- Co-relacionando tabelas

SELECT *
FROM public.scanned_amount Volumetria
JOIN public.shipment_nos IdViagem on Volumetria.shipment_no = IdViagem.shipment_no 
where IdViagem.shipment_no in ('DKGX25021700008',
'DKGX25021700037',
'DKGX25021700019',
'DKGX25021700014',
'DKGX25021600004',
'DKGX25021600031',
'DKGX25021600006',
'DKGX25021600002',
'SRTR22500315700',
'SRTR22500317751',
'SRTR22500325860')



-- Contagem de peças 17/02
SELECT
	COUNT(*) AS QUANTIDADE,
	DEST_SITE
FROM
	PUBLIC.SCANNED_AMOUNT
WHERE
	SHIPMENT_NO IN (
    'DKGX25022300004',
'DKGX25022400014',
'DKGX25022400037',
'DKGX25022400019',
'SRTR22500365440',
'SRTR22500363591',
'SRTR22500373740'
	)
	AND UF = 'MG'
	AND BILLCODE NOT LIKE '%-%'
GROUP BY
	DEST_SITE
ORDER BY
	QUANTIDADE DESC


-- ATUALIZAR DATA DE CHEGADA DE ID
UPDATE public.scanned_amount
SET final_estimated_arrival = '2025-03-27'
WHERE shipment_no = 'DKGX25032500018';


-- Filtrar Ids Troncais do dia
SELECT
	SHIPMENT_NO,
	SHIPMENT_STATE,
	ACTUAL_DEPARTURE_TIME
FROM
	PUBLIC.SHIPMENT_NOS
WHERE
	ACTUAL_DEPARTURE_TIME >= CURRENT_DATE - INTERVAL '1 DAY' + INTERVAL '3 HOURS' AND 	ACTUAL_DEPARTURE_TIME <= CURRENT_TIMESTAMP + INTERVAL '1 DAYS'

and shipment_no like 'DK%'

ORDER BY
	ACTUAL_DEPARTURE_TIME ASC

-- Filtrar Ids Secundários do dia

SELECT
	SHIPMENT_NO,
	SHIPMENT_STATE,
	ACTUAL_DEPARTURE_TIME
FROM
	PUBLIC.SHIPMENT_NOS
WHERE
	ACTUAL_DEPARTURE_TIME >= CURRENT_DATE + INTERVAL '0 HOURS' AND ACTUAL_DEPARTURE_TIME <= CURRENT_TIMESTAMP + INTERVAL '1 DAYS'

and shipment_no like 'SR%'

ORDER BY
	ACTUAL_DEPARTURE_TIME ASC

-- Filtrar ambos ID's
SELECT
	SHIPMENT_NO,
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