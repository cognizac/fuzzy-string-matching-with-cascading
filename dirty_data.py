import random
import re

__author__ = 'cognizac'

filein = open('new_project_data.csv')
fileout = open('dirty_project_data.csv', 'w')

fileout.write(filein.readline())

replace_dict = {'w': '\/\/', 'o': '0', 'l': '|', 'l': 'I', 'i': '|', 'i': 'l', 'a': '@', 'b': 'd', 'd': 'b', 'u': 'v',
                'v': 'u', 's': '$'}

def replacer(word):
    char_list = list(word)

    key_set = set(replace_dict.keys())
    char_set = set(list(word))

    intersect = key_set.intersection(char_set)

    for char in random.sample(intersect, random.randint(0, len(intersect))):
        word = word.replace(char, replace_dict[char])

    return word

def swapper(word):
    swap_choice = random.randint(0, len(word)-2)
    swap = list(word)
    swap[swap_choice], swap[swap_choice+1] = swap[swap_choice+1], swap[swap_choice]

    return ''.join(swap)

def capper(word):
    return ' '.join([word.capitalize() for word in word.split(' ')])


def deleter(word):
    num = random.randint(0, min(max(len(word)-3, 1)-1, 2))
    char_set = set(list(word))

    for char in random.sample(char_set, num):
        temp = list(word)
        temp.pop(word.index(char))
        word = ''.join(temp)

    return word

def space_remover(word):
    space_locations = [m.start() for m in re.finditer(' ', word)]

    chars = list(word)

    for space in random.sample(space_locations, random.randint(0, len(space_locations))):
        chars.pop(space)

    return ''.join(chars)

funcs = [swapper, replacer, capper, space_remover, deleter]

for line in filein.readlines():
    funcs_to_use = random.sample(funcs, random.randint(0, 3))

    data = line.strip().split(',')
    item_name = data[0]

    for func in funcs_to_use:
        item_name = func(item_name)

    out = str([item_name] + data[1:])[1:-1].replace('\'', '') + '\n'
    fileout.write(out)


filein.close()
fileout.close()
