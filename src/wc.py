# coding: utf-8
# file: wc.py
import dpark


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


def main():
    dc = dpark.DparkContext()
    options, args = dpark.optParser.parse_args()
    file_path = args[0]

    data = dc.textFile(file_path)
    wc = data.flatMap(parse_words)\
             .reduceByKey(lambda x, y: x + y)\
             .top(10, key=lambda x: x[1])
    print wc


if __name__ == '__main__':
    main()