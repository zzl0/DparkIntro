---
layout: post
title: DPark 漫谈 -- RDD
tags: DPark
comments: yes
og_image_url: ""
description: "本篇主要介绍 DPark 的核心概念 RDD."
date: 2014-10-26
---

注: 本篇是 [DPark 漫谈系列](./00-dpark.md)的第二篇.

## RDD from scratch

RDD（Resilient Distributed Dataset）是 DPark 的核心概念。
[上篇](./01-dpark-basic.md)中我们提到词频统计的示例，我们来尝试自己写一个 TextFileRDD。
这个 RDD 是由 dpark.textFile 方法产生。

### 基本的 RDD

首先我们需要知道文件的路径以及分块的大小，这样我们就可以把一个大文件分成多个小块来处理
（DPark 中我们把一个分块称之为 split），如下所示，我们把文件按照 splitSize 划分为
n 块儿。你可能注意到这里 splitSize 的命名方式是驼峰式的，我猜想是为了和 Spark
保持一致吧，后面我们会看到很多 RDD 的接口都是驼峰的方式。

``` python
class Split(object):
    def __init__(self, index):
        self.index = index

    def __str__(self):
        return 'Split<%s>' % self.index


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

    @property
    def splits(self):
        return self._splits
```

这里的 split 只是一个简单的索引，当调度程序要计算该 RDD 的一个分块时，
会调用它的 iterator 方法来获得一个分块的元素。由于 DPark 在处理文件时是按行来处理的，
所以当一个 split 的 end 边界值在文本行的中间位置时，该 split 会读取完整的行，
相应地，下一个 split 在读取数据时，要忽略（skip 变量的意义）已经被上一个 split 读取的部分。

``` python
    def iterator(self, split):
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
```

### 添加 map 接口

现在我们有了一个 RDD 但是没有为其增加一些编程接口方便我们使用，
下面我们来实现一个简单的 map 接口，该方法的返回值仍然是一个 RDD
（即 MappedRDD）。

``` python
class TextFileRDD(object):
    ...
    def map(self, f):
        return MappedRDD(self, f)


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
```

好了，写了这么多我们来测试一下代码（[完整代码](../src/rdd.py), [测试数据](../data/words.txt)）

```
$ python src/rdd.py
TextFileRDD
======= Split<0> =======
['Structure and Interpretation of Computer Programs\n', 'How to Design Programs\n']
======= Split<1> =======
['Python Programming: An Intro to CS\n', 'Concepts, Techniques, and Models of Computer Programming\n']
======= Split<2> =======
['On Lisp\n']

MappedRDD
======= Split<0> =======
[50, 23]
======= Split<1> =======
[35, 57]
======= Split<2> =======
[8]
```

### RDD 的特性

我们已经实现了两个简陋的 RDD，虽然样本较少，但是我们还是努力来总结一下 RDD 有哪些共性，

- RDD 可以由两种方式创建：1）存储设备中的数据(e.g 文件)；2）其他 RDD（e.g map 操作）。
- RDD 并不是一个计算好的数据集合，它只是包含了`源数据`（lineage，也翻译叫血统）是什么，以及`如何计算`，
而只是在需要的时候通过上面的信息计算数据集合。这样的一个好处就是当一个分块丢失时，
我们只需要根据这个分块的血统信息，重新构造出这部分数据。
- RDD 的分块信息，即有 split 相关的信息。

目前，我们差不多可以得到这么多信息，下面是我从 Spark 代码（很遗憾，DPark 的代码注释信息比较少）
中摘出来的关于 RDD 的注释，前三个我们的例子中都有涉及，但是最后两个并没有接触到，这是因为：

- 我们还没有实现 key-value 相关的操作（e.g. reduceByKey），DPark 允许我们自定义
partition 来对 key 进行分组。
- 关于 prefered locations，我们代码中也没有体现。但这的真实环境中比较重要，
例如我们可以得到一个文件在分布式文件系统中的位置，从而我们可以在运行的时候，
让任务运行在文件数据所在的机器上，提高系统性能。

```
A Resilient Distributed Dataset (RDD), the basic abstraction in MDpark.

Each RDD is characterized by five main properties:
- A list of splits (partitions)
- A function for computing each split
- A list of dependencies on other RDDS
- Optionally, a Partitioner for key-value RDDs (e.g hash-partitioned)
- Optionally, a list of prefered locations to compute each split on
```

## 词频统计剖析

[上篇](./01-dpark-basic.md)结束时，我们给出了一个词频统计的示例，
当时并没有解释其细节，这里我们将剖析其运行过程。

``` python
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

    data = dc.textFile(file_path, splitSize=2<<20)
    wc = data.flatMap(parse_words)\
             .reduceByKey(lambda x, y: x + y)\
             .top(10, key=lambda x: x[1])
    print wc


if __name__ == '__main__':
    main()
```

其 RDD 的逻辑图如下所示：

![wc rdd](/img/dpark/wc-rdd.png)

下面我们从 main 函数开始解释每一行的含义。

- `dc = dpark.DparkContext()`，DparkContext 是获得 DPark 功能的主要入口，
它设置了 DPark 一些重要的属性，并且可以用来创建 RDD 和广播一些大的数据集。

- 接下来两行利用了的 optParser，来获得我们从命令行传入的文件名称。
关于命令行参数需要注意的是 DPark 的框架所需要的参数要放在自己应用的参数前面。
例如，我们可以 `python wc.py -m mesos shakespeare.txt` 这样调用，
但是不可以 `python wc.py shakespeare.txt -m mesos`，
因为 DPark 的参数解析器看到第一个不能识别的参数时就会放弃对后面参数的解析，
之所以这样做是因为用户传的一些可选参数并不希望 DPark 去解析，
具体[参见](https://github.com/douban/dpark/blob/master/docs/faq.rst)。

- `data = dc.textFile(file_path, splitSize=2<<20)`，textFile 函数从 moosefs
文件系统或者本地文件系统读取文本文件，并返回一个 RDD。这里返回的是 TextFileRDD。
由于 RDD 都是惰性计算的，所以这个时候并没有进行任何数据的计算，只是生成了一个
TextFileRDD 实例，并在设置了分块大小等参数，我们使用的是 2M，
分块大小默认情况下是 64M，之所以改成这么小的值是因为我们的测试文本集只有 4.3M，
为了体现出分块的效果，所以设置为 2M。实际应用中这个参数的设置会影响程序的性能，
我们会在性能优化一节中来给一些建议，本篇中不用在意这点。


- flatMap 和 reduceByKey 都是 RDD 的变化操作。

- top 是一个动作。

## RDD 接口及其应用

## 小结

在这里我们去尝试实现一些简单的 RDD，从而理解其工作的原理，然后总结归纳出一些信息，
再来和实际情况对比，验证我们的理解以及不足。其实这也是我个人比较喜欢的一种学习方式，
我不想就给大家一个关于 RDD 是什么定义。这种方式有一个缺点（优点？）：我们会在总结归纳的时候犯错。

然后我们剖析了词频统计的例子，同时给出了 RDD 接口的一个应用示例。

## 练习

1. 平均响应时间
