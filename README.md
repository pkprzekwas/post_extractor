# Post Extractor (ZTNBD 2017)

Wymagania:
- python3
- build-essential
- pliki wejściowe*

Instalacja:
```bash
make init
```

Uruchomienie:
```bash
make run
```


*Pliki wejściowe w formacie json należy umieścić w katalogu `data/in`. Znaleźć je można w katalogu `statement` danych przykładowych znajdujących się na chmurze google.


### TODO
Dostosowanie skryptu do działania w pysparku.
* Stworzenie spark contextu 
* Odczyt danych z HDFS zamiast lokalnego dysku
* Zrównoleglenie wybranych procesów
