# coding: utf-8
import dpark


def set_diff(rdd1, rdd2):
    """
    Return an RDD with elements in rdd1 but not in rdd2.
    """
    pair_rdd1 = rdd1.map(lambda x: (x, None))
    pair_rdd2 = rdd2.map(lambda x: (x, 1))
    return pair_rdd1.leftOuterJoin(pair_rdd2)\
                    .filter(lambda x: not x[1][1])\
                    .map(lambda x: x[0])


if __name__ == '__main__':
    rdd1 = dpark.parallelize([1, 2, 3, 4])
    rdd2 = dpark.parallelize([3, 4, 5, 6])
    diff = set_diff(rdd1, rdd2)
    rs = diff.collect()
    assert sorted(rs) == [1, 2]  # DPark 不保证顺序
