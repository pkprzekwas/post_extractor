# Projekt ZTNBD

## Lokalna instalacja
###### Wymagania
- docker
- ~4GB wolnego miejsca na dysku
- zalecany linux + build-essential

###### Linux 
```bash
make notebook
```
###### Others (nie sprawdzane)
```bash
# lokalizacja - główny katalogu projektu
docker run -it --rm -v $(pwd):/home/jovyan/work -p 8888:8888 jupyter/pyspark-notebook
```

Komenda startuje kontener dockerowy z Jupyterem i podmontowuje katalog projektu.
Moduły znajdują się w katalogu `modules` i tam też będą lądowały kolejne.
Uruchomienie póki co możliwe jest tylko z poziomu notebook'a.

## Moduły


### Posty

##### PostTransformer
Klasa PostTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol, pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta wydobywa z kolumny inputCol, z formatu json, content i umieszcza go w kolumnie outputCol w postaci tekstu.

##### TranslateTransformer
Klasa TranslateTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol, pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta tłumaczy tekst zawarty w kolumnie inputCol z języka polskiego na angielski i umieszcza go w kolumnie outputCol.

##### SentenceTransformer
Klasa SentenceTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol, pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta dzieli tekst zawarty w kolumnie inputCol na zdania i umieszcza go w kolumnie outputCol w postaci tablicy tekstów.

##### SpeechPartsTransformer
Klasa SpeechPartsTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol, pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta z listy tekstów zawartych w kolumnie inputCol, zlicza wystąpienia części mowy i wstawia do outputCol w postaci słownika, którego kluczem jest tag części mowy wg projektu [Penn Treebank](http://www.ling.upenn.edu/courses/Fall_2003/ling001/penn_treebank_pos.html), a wartością lista ilości wystąpień danej części mowy w danym tekście.

##### SentimentTransformer
Klasa SentimentTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol, pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta z listy tekstów zawartych w kolumnie inputCol, wylicza sentymenty tekstów i wstawia do outputCol w postaci słownika, którego kluczami są polaryzacja oraz subiektywność, a wartościami listy wartości odpowiednich pól sentymentów dla danego tekstu.


### Featury

##### FeatureTransformer
Klasa FeatureTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
Metoda ta wydobywa zawartość z kolumny inputCol, z formatu json i umieszcza go w kolumnie outputCol w postaci słownika, którego kluczem jest nazwa featura, a wartością lista wartości danego featura, w kolejnych elementach jsonowych.


### Uniwersalne

##### MaxTransformer
Klasa MaxTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
ta wylicza maksimum z listy wartości, dla wszystkich kluczy znajdujących się w słowniku zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci słownika klucz - maksimum wartości.

#####  MeanTransformer
Klasa MeanTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
ta wylicza średnią z listy wartości, dla wszystkich kluczy znajdujących się w słowniku zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci słownika klucz - średnia wartości.

##### MedianTransformer
Klasa MedianTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
ta wylicza medianę z listy wartości, dla wszystkich kluczy znajdujących się w słowniku zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci słownika klucz - wartość środkowa.

##### NumberOfOccurrencesTransformer
Klasa NumberOfOccurrencesTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
ta wylicza długość listy wartości, dla wszystkich kluczy znajdujących się w słowniku zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci słownika klucz - liczba elementów listy wartości.

##### ConvertDictToListTransformer
Klasa ConvertDictToListTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowe parametry: keys, który zawiera listę nazw kluczy,
które mają zostać skonwertowane w podanej kolejności do formatu listy oraz element_type, który określa typ elementu słownika. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta wydobywa zawartość z kolumny inputCol, w formacie słownika i umieszcza go w kolumnie outputCol w postaci listy wartości kluczy podanych w parametrze. W przypadku braku istnienia danego klucza w słowniku, zwracana wartość będzie równa None.

##### SelectRecordsTransformer
Klasa SelectRecordsTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowe parametry: keys, który zawiera listę nazw kluczy,
które mają zostać skonwertowane w podanej kolejności do formatu listy oraz element_type, który określa typ elementu słownika. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta wydobywa zawartość z kolumny inputCol, w formacie słownika i umieszcza go w kolumnie outputCol w postaci słownika z przefiltrowanymi kluczami podanymi w parametrze. W przypadku braku istnienia danego klucza w słowniku, zwracana wartość będzie równa None.
