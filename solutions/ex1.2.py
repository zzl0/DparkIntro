# coding: utf-8

def parse_words(line):
    """
    解析文本行，提取其中的word，并计数为1.

    注意:
    - 这里假设word是由于英文字母组成的字符串.
    - 该函数没有优化速度.
    """
    for w in line.split():
        if w.isalpha():
            yield (w, 1)


def flat_map(data, func):
    """
    >>> flat_map(["this is a test", "this is another test"], parse_words)
    [('this', 1), ('is', 1), ('a', 1), ('test', 1), ('this', 1), ('is', 1), ('another', 1), ('test', 1)]
    """
    def flat(lst):
        return [y for x in lst for y in x]
    return flat(map(func, data))


if __name__ == '__main__':
    import doctest
    doctest.testmod()
