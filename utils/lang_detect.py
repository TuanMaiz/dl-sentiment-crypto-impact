from langdetect import detect


class LangUtils:
    def detect_english(text):
        try:
            return detect(text) == 'en'
        except:
            return False