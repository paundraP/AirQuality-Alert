import requests
url = "https://ispu.kemenlh.go.id/apimobile/v1/stationDetail?id=KOTA_PROBOLINGGO"
res = requests.get(url)
print(res.status_code)
print(res.text[:200])
