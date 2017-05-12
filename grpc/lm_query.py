# -*- coding: utf-8 -*-

from twkorean import TwitterKoreanProcessor
import kenlm
import os, sys

def main():
    if len(sys.argv) != 2:
        print("query_lm.py <arpa_file>")
        exit(0)

    lm_scoring = LMScoring(sys.argv[1])

    while True:
        sent = raw_input("sentence:")
        score = lm_scoring.query(sent)
        print("score: %s" % score)

class LMScoring:
    def __init__(self, model_file):
        self.model = kenlm.Model(model_file)
        self.tokenizer = TwitterKoreanProcessor(stemming=False)

    def query(self, sentence):
        #tokens = self.tokenizer.tokenize_to_strings(sentence)
        #tokenized_sentence = " ".join(tokens)
        score = self.model.score(sentence, bos=True, eos=True)
        return score


if __name__ == "__main__":
    main()
