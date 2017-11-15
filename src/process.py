from collections import OrderedDict
from functools import reduce
from operator import methodcaller

from processing_modules.speech_parts_module import SpeechPartsModule
from processing_modules.sentence_module import SentenceModule
from processing_modules.sentiment_module import SentimentModule


def process(data):

    speech_parts = SpeechPartsModule().run(data)
    sentences = SentenceModule().run(data)
    sentiments = SentimentModule().run(data)

    avg_sentiment = reduce(lambda x, y: x + y, sentiments)
    avg_sentiment /= len(sentiments)

    sentences_to_str = list(map(str, sentences))
    sentences_to_words = list(map(methodcaller('split', ' '), sentences_to_str))
    sentences_len = list(map(len, sentences_to_words))

    avg_sentence_len = reduce(lambda x, y: x + y, sentences_len) / len(sentences)
    max_sentence_len = reduce(lambda x, y: max(x, y), sentences_len)
    min_sentence_len = reduce(lambda x, y: min(x, y), sentences_len)

    feature_json = OrderedDict({
        'number_of_sentences': len(sentences),
        'most_used_speech_part': next(iter(speech_parts)),
        'average_sentiment': avg_sentiment,
        'max_sentiment': max(sentiments),
        'min_sentiment': min(sentiments),
        'parts_of_speech': speech_parts,
        'sentences_with_sentiments': {
            str(ste): sti for ste, sti in zip(sentences, sentiments)
        },
        'avg_sentence_len': avg_sentence_len,
        'min_sentence_len': min_sentence_len,
        'max_sentence_len': max_sentence_len,
    })

    return feature_json
