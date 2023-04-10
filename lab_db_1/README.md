#Інструкція

1. Необхідно завантажити всі необхідні файли до python проекту:

   Структура файлів: docker-compose.yml, README.md, папка app

   Папка app: Dockerfile, main.py, requirements.txt, папка data

   В папку data необхідно завантажити наступні файли: Odata2019File.csv, Odata2020File.csv

2. В налаштуваннях Docker необхідно дозволити file sharing вказавши шлях до папки app:

   Settings - Resources - File sharing - your_path - Apply & restart

3. Для запуску виконати наступну команду:

```bach
docker-compose build --no-cache && docker-compose up -d --force-recreate
```
Або
```bach
(docker-compose build --no-cache) -and (docker-compose up -d --force-recreate)
```

4. Після виконання роботи, створяться файли ZnoMathResults.csv та TimeRecord.txt у папці data:

   ZnoMathResults.csv - файл з порівняльною характеристикою згідно 8-го варіанту

   TimeRecord.txt - час запису даних до БД