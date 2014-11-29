__author__ = 'cognizac'

from fuzzywuzzy import process

#use a set for fast retrieval, can use lists to slow down a bit for more dramatic improvments using hadoop
choices = {'bread', 'beer', 'butter', 'milk', 'eggs', '1 gal water', '2 liter cola', '2 liter diet cola',
                      'orange juice', 'apple juice', '5 lb bag of apples', 'bananas', 'bag of grapes', 'tonic water',
                      'onions', 'bell peppers', 'celery', 'coffee', 'swiss cheese', 'cheddar cheese', 'flour',
                      'ramen', 'jalapenos', 'chocolates', 'gummi bears', 'cookies', 'cakes', 'seltzer', 'bitters',
                      'whiskey', 'vodka', 'spaghetti', 'almonds', 'peanuts', 'pears', 'cashews', 'peaches', 'apricots',
                      'licorice', 'bubble gum', 'strawberries', 'blueberries', 'raspberries', 'mangos', 'lettuce',
                      'spinach', 'swiss chard', 'kale'}
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


good_data = open('new_project_data.csv')
bad_data = open('dirty_project_data.csv')

good_data.readline()
bad_data.readline()

count = 0
good_count = 0

for line in good_data.readlines():
    good = line.strip().split(',')[0]
    bad = bad_data.readline().strip().split(',')[0]
    match = get_match(bad)

    #print bad + ' -> ' + match + ' : ' + good

    if match == good:
        good_count += 1

    count += 1

print str(float(good_count)/float(count)*100) + '% correct'