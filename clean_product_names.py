__author__ = 'cloudera'

#tested with local_run.sh

from pycascading.helpers import *

import sys
sys.path.extend('/usr/lib/python2.6/site-packages/')
from fuzzywuzzy import process


#use a set for fast retrieval, can use lists to slow down a bit for more dramatic improvments using hadoop
choices = set(['bread', 'beer', 'butter', 'milk', 'eggs', '1 gal water', '2 liter cola', '2 liter diet cola',
                      'orange juice', 'apple juice', '5 lb bag of apples', 'bananas', 'bag of grapes', 'tonic water',
                      'onions', 'bell peppers', 'celery', 'coffee', 'swiss cheese', 'cheddar cheese', 'flour',
                      'ramen', 'jalapenos', 'chocolates', 'gummi bears', 'cookies', 'cakes', 'seltzer', 'bitters',
                      'whiskey', 'vodka', 'spaghetti', 'almonds', 'peanuts', 'pears', 'cashews', 'peaches', 'apricots',
                      'licorice', 'bubble gum', 'strawberries', 'blueberries', 'raspberries', 'mangos', 'lettuce',
                      'spinach', 'swiss chard', 'kale'])
def get_match(word):
    results = process.extract(word, choices)
    top_score = results[0][1]

    best_matches = [index for index, item in enumerate(results) if item[1] == top_score]

    if len(best_matches) == 1:
        return results[best_matches[0]][0]
    elif len(best_matches) > 1:
        min_length_diff = None
        min_length_index = 0
        for index in best_matches:
            diff = abs(len(results[index]) - len(word))
            if diff < min_length_diff:
                min_length_diff = diff
                min_length_index = index

        return results[min_length_index][0]
    else:
        print word
        raise Exception('No match found!')


# good_data = open('new_project_data.csv')
# bad_data = open('dirty_project_data.csv')
#
# good_data.readline()
# bad_data.readline()
#
# count = 0
# good_count = 0
#
# for line in good_data.readlines():
#     good = line.strip().split(',')[0]
#     bad = bad_data.readline().strip().split(',')[0]
#     match = get_match(bad)
#
#     #print bad + ' -> ' + match + ' : ' + good
#
#     if match == good:
#         good_count += 1
#
#     count += 1
#
# print str(float(good_count)/float(count)*100) + '% correct'


@udf_map(produces=['cleanedTuple', 'date', 'item_amount'])
def clean_product_names(tuple):
    data = tuple.get(1).split(',')
    clean_product_name = get_match(data[0])
    yield [clean_product_name, data[1], data[2]]


def main():
    flow = Flow()
    # The TextLine() scheme produces tuples where the first field is the
    # offset of the line in the file, and the second is the line as a string.
    input = flow.source(Hfs(TextLine(), '/home/cloudera/PyCharmProjects/fuzzy-string-matching-with-cascading-github/dirty_project_data_short.csv'))
    output = flow.tsv_sink('/home/cloudera/PyCharmProjects/fuzzy-string-matching-with-cascading-github/out')

    input | clean_product_names | output

    flow.run(num_reducers=1)