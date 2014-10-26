# coding: utf-8
import os


class TextFileRDD(object):
    DEFAULT_SPLIT_SIZE = 64 << 20  # 64M

    def __init__(self, path, splitSize=None):
        self.path = path

        if splitSize is None:
            splitSize = self.DEFAULT_SPLIT_SIZE
        self.splitSize = splitSize

        size = os.path.getsize(path)
        n = size / splitSize
        if size % splitSize > 0:
            n += 1
        self._splits = [Split(i) for i in range(n)]

    def __str__(self):
        return 'TextFileRDD'

    @property
    def splits(self):
        return self._splits

    def compute(self, split):
        with open(self.path) as f:
            start = split.index * self.splitSize
            end = start + self.splitSize
            if start > 0:
                f.seek(start - 1)
                ch = f.read(1)
                skip = ch != '\n'
            else:
                f.seek(start)
                skip = False

            for line in f:
                if start >= end:
                    break
                start += len(line)
                if skip:
                    skip = False
                else:
                    yield line

    def map(self, f):
        return MappedRDD(self, f)


class Split(object):
    def __init__(self, index):
        self.index = index

    def __str__(self):
        return 'Split<%s>' % self.index


class MappedRDD(object):

    def __init__(self, prev, f):
        self.prev = prev
        self.f = f

    def __str__(self):
        return 'MappedRDD'

    @property
    def splits(self):
        return self.prev.splits

    def compute(self, split):
        return (self.f(x) for x in self.prev.compute(split))


if __name__ == '__main__':
    def print_rdd(rdd):
        print rdd
        for split in rdd.splits:
            print '======= %s =======' % split
            print list(rdd.compute(split))
        print

    rdd = TextFileRDD('data/words.txt', splitSize=60)
    print_rdd(rdd)

    rdd = rdd.map(lambda x: len(x))
    print_rdd(rdd)
