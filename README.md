# Post Extractor (ZTNBD 2017)

### Lokalna instalacja
Wymagania:
- python3
- build-essential
- pliki wejściowe*

1. Instalacja:
```bash
make init
```
2. Uruchomienie:
```bash
make run
```

### Jupyter on docker
1. Instalacja [dockera](https://docs.docker.com/engine/installation/).
2. Start Jupytera. Komenda uruchamia usługę Jupyter na porcie 8888. Token do autoryzacji wyświetla się w logach komendy uruchamiającej kontener.
```bash
docker run -it --rm -p 8888:8888 jupyter/pyspark-notebook
```
3. Po zalogowaniu do głównego panelu Jupytera należy stworzyć sesję w terminalu. 
```bash
prawy górny róg --> New --> Terminal
```
4. Kopiowanie repozytorium z użyciem git'a.
```bash
git clone https://github.com/pkprzekwas/post_extractor.git
```
5. Kolejnym krokiem jest umieszczenie plików wejściowych w katalogu `post_extractor/data/in`*.
6. Następnie można uruchomić plik z rozszerzeniem `*.ipynb` i wykonać kod znajdujący się w poszczególnych komórkach.

*Pliki wejściowe w formacie json należy umieścić w katalogu `data/in`. Znaleźć je można w katalogu `statement` danych przykładowych znajdujących się na chmurze google.

#### TODO
Integracja z Apache Spark.