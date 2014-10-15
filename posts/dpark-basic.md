---
layout: post
title: DPark 漫谈 -- DPark 简介
tags: DPark
comments: yes
og_image_url: ""
description: "本篇主要介绍了什么是 DPark，为什么用 DPark，以及 DPark 的使用示例"
---

注: 本篇是 [DPark 漫谈系列](/2014/08/05/dpark/)的第一篇.

## 什么是 DPark

DPark 是一个基于 Mesos 的集群计算框架 (cluster computing framework)，
是 [Spark](http://spark.apache.org/) 的 Python 实现版本，类似于 MapReduce，
但是比其更灵活，可以用Python非常方便地进行分布式计算，并且提供了更多的功能以便更好的进行迭代式计算。

八卦： DPark 是 Davies 在豆瓣时的作品之一，据 Davies 自己在 Spark 的邮件列表里所讲，
第一版的 Dpark 用了一周的时间。

## Why DPark

下面我们通过一个问题引出 DPark。

### 问题描述

假设现在我们有 Apache web Server 的 log 如下所示，其中最后一行是响应的字节数。
我们的问题就是统计一下一天内网站的流量，即对最后一列进行求和。

```
81.107.39.38 -  ... "GET /ply/ HTTP/1.1" 200 7587
81.107.39.38 -  ... "GET /favicon.ico HTTP/1.1" 404 133
81.107.39.38 -  ... "GET /ply/bookplug.gif HTTP/1.1" 200 23903
81.107.39.38 -  ... "GET /ply/ply.html HTTP/1.1" 200 97238
81.107.39.38 -  ... "GET /ply/example.html HTTP/1.1" 200 2359
66.249.72.134 - ... "GET /index.html HTTP/1.1" 200 4447
```

### 基本 python 程序

``` python
wwwlog = open("access-log")
total = 0
for line in wwwlog:
    bytestr = line.rsplit(None,1)[1]
    if bytestr != '-':  # 忽略 apache 缺省值
        total += int(bytestr)
print "Total", total
```

如果数据量较小，我们可以单机完成，但是随着数据量的进一步加大，我们可能就需要使用多进程，
甚至多台机器进行分布式处理。 在分布式环境下，我们会遇到下面的一些问题：

1. 自己对数据进行分块。
2. 合并每个分块的结果。
3. 网络通信。
4. 遇到错误（网络、机器错误）时重试任务。
5. ...

想想就够复杂的，最重要的是，在遇到下一个任务时，可能又要写一遍这些与业务没有关系代码。
所以一个很显然的方法就是把这些代码抽象成一个计算框架，以后只是方便地使用就可以。

### 函数式方式

在说明如何抽象之前，我们来看看把上面的代码改为函数式方式是什么样的。

``` python
wwwlog = open("access-log")
bytecolumn = map(lambda x: x.rsplit(None,1)[1], wwwlog)
bytes = map(lambda x: int(x) if x != '-' else 0, bytecolumn)
print "Total", sum(bytes)
```

函数式风格强调的是不变性，就像数学函数一样，给定同样的输入，产生相同的输出。上述代码也是，
我们不再通过改变变量的状态 (total) 得到结果，而是对数据进行一系列变换，最后得到结果。

我们可以用下面的图来描述函数式的处理方式

![pipeline](/img/dpark/pipeline.png)

我们可以把它看做一个数据处理的 pipeline，在 pipeline 的每一个步骤我们声明了一个操作
（对应为代码中的 lambda 函数），这个操作将作用在整个输入流上。

这样做的好处有:

  - 我们不用在行的级别来思考问题，并更新状态变量。而是在整个文件的级别考虑数据的变换。
  - 这个更像是一个声明式的风格
  - Key: Think big ...

如果我们设计了一个计算框架，提供了 map、reduce 接口（上例中是sum，
但是 reduce 是一个更一般的聚合方法，sum 可以有 reduce 实现出来，
例如针对上面的例子 `sum = lambda x: reduce(lambda a,b: a+b, x)`），
用 map、reduce 影藏了网络、错误恢复等业务无关的逻辑，
那么上述的代码就是分布式的代码了，可以处理大量日志数据了。

Hadoop MR 基本思想就是这样，在 Hadoop MR 中 bytecolumn、bytes
这样的中间数据是会落到磁盘上的。这也就给我们带来了一个问题，
我们能不能省掉这些中间的数据呢？

### generate 方式

如何省掉中间数据呢，我们先看看 python 是怎么做的，
很多前辈注意到 list comprehension 会产生中间列表，浪费空间。其中
Peter Norvig 在他的文章 [Python Proposal: Accumulation Displays](http://norvig.com/pyacc.html)
中提出来一种解决方式，促进了这个问题的进一步讨论，最终 generate expression
获得最终的胜利。下面看看 generate 的方式是怎样的：

``` python
wwwlog = open("access-log")
bytecolumn = (line.rsplit(None,1)[1] for line in wwwlog)
bytes = (int(x) for x in bytecolumn if x != '-')
print "Total", sum(bytes)
```

其实和前面的差不多，只不过中间的结果都是一个惰性的数据集，在需要的时候才会计算。
如果把 bytecolumn、bytes 这样的惰性数据集抽象成一个类 LazyData，然后为
LazyData 实现相应的 map、reduce 等操作的接口封装分布式相关任务，
我们又得到了一个分布式的计算库。

事实上 DPark 正是这么做的，只不过 LazyData 被称之为 RDD (Resilient Distributed Dataset)，
而且多了一些接口，如 flatMap、filter、reduceByKey、collect 等，这些后面用到时都会介绍。

### dpark 方式

``` python
wwwlog = dpark.textFile("access-log")
bytecolumn = wwwlog.map(lambda x: x.rsplit(None,1)[1])
bytes = bytecolumn.map(lambda x: int(x) if x != '-' else 0)
print "Totoal", bytes.reduce(lambda x,y: x+y)
```

这个其实和函数式方式很像的。

最后强调一下，这里只是从一个侧面来引出 DPark，高层次说起来就是这么简单的，但是细节是魔鬼，
后面我们再来和细节战斗吧。

## 应用示例

本章结束之前，我们来看一下常用词频统计任务，这虽然是一个简单的任务，但它不是一个假想的任务，
词频统计是大部分自然语言处理任务的基础步骤。从这个程序中，也可以感受下 DPark 代码的风格
（其实是我自己的代码习惯了 ^_^）。我没有期望你现在都理解这个代码的全部含义，先看看吧。

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

    data = dc.textFile(file_path)
    wc = data.flatMap(parse_words)\
             .reduceByKey(lambda x, y: x + y)\
             .top(10, key=lambda x: x[1])
    print wc


if __name__ == '__main__':
    main()
```

执行结果如下：

```
$ python wc.py -m mesos shakespeare.txt
2014-10-13 20:52:15,255 [INFO] [root     ] Enter, port=0.
2014-10-13 20:52:15,255 [INFO] [root     ] Enter.
...
2014-10-13 20:52:18,830 [INFO] [dpark.job] read 4.3MB (0% localized)
[('the', 23272), ('I', 20041), ('and', 16817), ('to', 15506), ('of', 15037), ('you', 12361), ('a', 12155), ('my', 10686), ('in', 9471), ('is', 8318)]
```

我用的文本是[莎士比亚的文章](http://norvig.com/ngrams/shakespeare.txt)，
作为对比大家可以看看普通人对词汇的使用[频率](http://norvig.com/mayzner.html)，
从结果可以看出莎士比亚在这部作品中更喜欢 'I', 'my' 等词汇，
这也说明每个人在词汇的使用频率上有个人的喜好的，这种差异也是拼音输入法个性化的基础。
另外，这些排名靠前的都是虚词（或代词），没有多大实际意义，在很多应用中会把这些词汇去掉。

## 小结

本文简单介绍了 DPark 是什么，并通过一个示例一步一步引出为什么要 DPpark，
最后给出了一个真实的使用场景。