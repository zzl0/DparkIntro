# coding: utf-8

from __future__ import division


def max_min_fairness(demands, capacity):
    demands = sorted(demands, key=lambda x: x[1])
    n = len(demands)
    rs = [capacity / n] * n
    for i, (u, v) in enumerate(demands):
        if rs[i] > v:
            total, rs[i] = rs[i] - v, v
            unit = total / (n - i - 1)
            for j in range(i+1, n):
                rs[j] += unit
            print '%d %.2f %.2f %s' % (i, total, unit, rs)
        else:
            break
    users = [x[0] for x in demands]
    return zip(users, rs)


def main():
    demands = [('u1', 2), ('u2', 2.6), ('u3', 4), ('u4', 5)]
    capacity = 10
    print max_min_fairness(demands, capacity)


if __name__ == '__main__':
    main()

# python src/mesos/max_min_fair.py
# 0 0.50 0.17 [2, 2.6666666666666665, 2.6666666666666665, 2.6666666666666665]
# 1 0.07 0.03 [2, 2.6, 2.6999999999999997, 2.6999999999999997]
