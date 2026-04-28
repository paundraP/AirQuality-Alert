sumber: ` https://ispu.kemenlh.go.id/apimobile/v1/getStations`

contoh: `https://ispu.kemenlh.go.id/apimobile/v1/getDetail/stasiun/KABUPATEN_TUBAN`

1. ambil kota di jawa timur yang tersedia, tulis id_stasiun nya di array `DAERAH_JATIM`

2. ganti url ke `https://ispu.kemenlh.go.id/apimobile/v1/getDetail/stasiun/<id_station>`

3. penyesuaian payload:
    - buat ambil aqi itu variabel name e `val`
    - buat ambil pm25 nama variabel tetap `pm25`
