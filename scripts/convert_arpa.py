#!/usr/env python
#-*- coding: utf-8 -*-
import sys
import os
from snakebite.client import Client
from optparse import OptionParser

HADOOP_HOST = "local"

def main(opts, args):
    hadoop_host = HADOOP_HOST
    hadoop_user_dir = None
    if opts.hdfs:
        print("hdfs enter")
        if opts.host:
            hadoop_host = opts.host
        hadoop_user_dir = opts.hdfs

    uni_gram_cnt = 0
    bi_gram_cnt = 0
    tri_gram_cnt = 0
    four_gram_cnt = 0
    five_gram_cnt = 0

    result_buffer = []
    source_input = None
    if not hadoop_user_dir:
        if len(args) > 2:
            source_input = sys.argv[1]
        else:
            source_input = sys.stdin

        for line in source_input:
            result_buffer.append(line)

            items = line.split()
            items_cnt = len(items)
            if items_cnt == 3: # 1-grams
                uni_gram_cnt +=1
            elif items_cnt == 4: #2-grams
                bi_gram_cnt += 1
            elif items_cnt == 5: #3-grams
                tri_gram_cnt += 1
            elif items_cnt == 6:
                four_gram_cnt += 1
            elif items_cnt == 7:
                five_gram_cnt +=1

    else:
        print "connect to haddoop"
        hadoop_client = Client(hadoop_host, 8020, use_trash=False)
        for g in hadoop_client.cat([os.path.join(hadoop_user_dir, "*.txt")]):
            for line in g:
                result_buffer.append(line)

                items = line.split()
                items_cnt = len(items)
                if items_cnt == 3: # 1-grams
                    uni_gram_cnt +=1
                elif items_cnt == 4: #2-grams
                    bi_gram_cnt += 1
                elif items_cnt == 5: #3-grams
                    tri_gram_cnt += 1
                elif items_cnt == 6:
                    four_gram_cnt += 1
                elif items_cnt == 7:
                    five_gram_cnt +=1


    print('\\data\\')
    if uni_gram_cnt != 0:
        print("ngram 1=%s" % uni_gram_cnt)

    if bi_gram_cnt != 0:
        print("ngram 2=%s" % bi_gram_cnt)

    if tri_gram_cnt != 0:
        print("ngram 3=%s" % tri_gram_cnt)

    if four_gram_cnt != 0:
        print("ngram 4=%s" % four_gram_cnt)

    if five_gram_cnt != 0:
        print("ngram 5=%s" % five_gram_cnt)

    result_iter = iter(result_buffer)
    print
    print_ngram(result_iter, 1, uni_gram_cnt)
    print
    print_ngram(result_iter, 2, bi_gram_cnt)
    print
    print_ngram(result_iter, 3, tri_gram_cnt)
    print
    print("\\end\\")

def print_ngram(buffer_iterator, n, gram_cnt):
    if gram_cnt != 0:
        print("\\%s-grams:" % n)
        for _ in range(gram_cnt):
            print(buffer_iterator.next()),

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-o", "--host", help="hadoop host", default=False)
    parser.add_option("-d", "--hdfs", help="read from hdfs", default=False)
    (opts, args) = parser.parse_args()
    print(opts)
    main(opts, args)
