import requests

def get_mg_bases():

    _dict = {}

    url = "https://gw.jtjms-br.com/basicdata/network/select"
    params = {
        "current": 1,
        "size": 500,
        "name": "cd",
        'networkId': 137,
        'queryLevel': 3
    }
    header = {
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
        "authToken": "ccac5266684c4f2a8703b121b0cfb3cb",
        "lang": "PT",
        "langType": "PT",
        "routeName": "unitArea",
        "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "timezone": "GMT-0300"
    }

    response = requests.get(url = url, params = params, headers = header)

    for record in response.json()["data"]["records"]:
        _dict[record["name"]] = record["financialCenterDesc"]

    print(_dict)

get_mg_bases()


