import re
import nltk
from nltk.corpus import stopwords

ps = nltk.stem.PorterStemmer()
stop_words_set = set(stopwords.words("english"))

# steps:
# replace common punctuation (,.) with spaces
# filter is word (full match [a-zA-Z']+)
# regularize case (lowercase)
# filter is not stop word     
# apply stemmer       
def stem(word):
    word = word.replace(",", " ")
    word = word.replace(".", " ")
    if not re.fullmatch("[a-zA-Z']{2,}", word):
        return None
    word = word.lower()
    if word in stop_words_set:
        return None
    return ps.stem(word)