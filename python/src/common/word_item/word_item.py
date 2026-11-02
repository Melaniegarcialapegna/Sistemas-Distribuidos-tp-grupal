class WordItem:
    def __init__(self, word, eng_count, fre_count):
        self.word = word
        self.eng_count = eng_count
        self.fre_count = fre_count

    @staticmethod
    def create_in_language_word_count(word, in_language):
        if in_language == "en":
            return WordItem(word, 1, 0)
        elif in_language == "fr":
            return WordItem(word, 0, 1)
        else:
            raise ValueError(f"Unsupported language: {in_language}")

    def intersection_reached(self):
        return self.eng_count >= 1 and self.fre_count >= 1
    
    def __lt__(self, other):
        return (self.eng_count + self.fre_count) < (other.eng_count + other.fre_count)

    def __add__(self, other):
        if self.word != other.word:
            raise ValueError("Cannot add WordItem objects with different words")
        return WordItem(self.word, self.eng_count + other.eng_count, self.fre_count + other.fre_count)

    def __gt__(self, other):
        return (self.eng_count + self.fre_count) > (other.eng_count + other.fre_count)

    def __eq__(self, other):
        return (self.eng_count + self.fre_count) == (other.eng_count + other.fre_count)

    def total_count(self):
        return self.eng_count + self.fre_count