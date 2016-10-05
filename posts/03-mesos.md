---
layout: post
title: DPark 漫谈 -- Mesos 简介
tags: DPark
comments: yes
og_image_url: ""
description: "本篇首先介绍 Mesos 基础概念，然后实现一个简单的 Mesos Framework"
date: 2014-10-26
---

注: 本篇是 [DPark 漫谈系列](./00-dpark.md)的第三篇.

## 什么是 Mesos

DPark 是一个分布式的计算框架（Framework），他的“分布式”部分主要是基于 Mesos 实现的。
所以在讲解 DPark 的 Mesos Scheduler 之前，我们需要了解什么是 Mesos，以及 Mesos
的特点。

Mesos 2010 年是诞生于 Berkeley AMP lab，用于分布式集群的资源管理，
抽象掉了底层的物理机器，允许我们在同一集群中执行多种不同的计算框架，
比如 Spark、DPark、MPI、Hadoop 等，Mesos 最初使用 containerizer 进行资源隔离。

那么为什么需要在同一个集群中运行多种不同的计算框架？第一，没有一个分布式计算框架可以满足我们的所有需求，所以我们有部署多种计算框架的需求；第二，如果为每一个计算框架单独部署一个集群的话，数据没法共享，计算资源得不到充分的利用。当然对于有钱任性的公司来说，这都不是事儿。

另外，我们也可以从 datacenter 的角度来看看 Mesos 所处的位置，随着云计算的普及，
“the datacenter is the new computer” 已经得到了比较多的认可，
那么这个新的计算机就需要一个新的操作系统，就像我们的单个计算机上都有一个操作系统一样，
它需要为我们提供下面 4 种功能（这个图来自 [Anthony D. Joseph 的 slide](http://laser.inf.ethz.ch/2013/material/joseph/LASER-Joseph-2.pdf)）。

![dcos](/img/mesos/dcos.png)

Mesos 解决的就是其中资源管理问题，资源包括 memory、cpu、disk、bandwidth 等。上图中的其他三个方面本文不会过多涉及，我们这里只列出一些具有代表性的例子，
方便大家有个感性的认识。

- Data Sharing: HDFS, S3, Moosefs
- Programming Abstractions: Zookeeper
- Debugging & Monitoring: Dapper

## Mesos 的设计

1. 较高的资源利用率
2. 支持多种分布式框架（Spark、MapReduce 等，以及未来的计算框架）
3. 支持 10,000 个节点
4. 高可用

面对这些目标，Mesos 选择了两级的调度方式，Mesos 负责资源管理、分配、以及高可用，
而把 task 的调度逻辑交给各个计算框架自己实现。这样可以让 Mesos 的核心逻辑比较简单，
同时给予了计算框架更多的自由。计算框架需要实现的主要是两个部分：

- Scheduler：从 Mesos master 接收资源，生成 task，返回给 master，并且管理 task 的执行结果。
- Executor：从 Mesos slave 接受 task description、生成 task 并执行，以及汇报执行结果。（注：Executor 是可选的，Mesos 自带了一些基本的 Executor，如 CommandExecutor）

![architecture](/img/mesos/architecture.png)

上图是 Mesos 的架构图，Mesos 是一个 master/slave 的架构，使用了 ZooKeeper 在多个
master 之间选主，从而实现 master 的高可用。

### Resource Offer

刚才我们提到计算框架自己实现的 Scheduler 会从 Mesos master 那里接收资源，
Mesos 把这个资源抽象为 resource offer，它包含了资源的类型以及数量、该资源位于哪个节点等信息。这些资源信息是由 Mesos slave 节点上报给 Mesos master，然后 Mesos master 按照某种策略（如 DRF: Dominant Resource Fairness）把资源分配给各个计算框架。下图提供了一个从 resource offer 到 running task 的示例。

1. slave 1 向 master 汇报说它有 4 个 cpu 以及 4 GB 的内存资源可用。
2. master 根据资源分配策略把这个资源信息以 resource offer 的形式发送给 Framework1。
3. Framework1 根据接受到的 resource offer 创建了两个 task description，第一个使用了 <2 CPUS, 1 GB RAM>，第二个使用了 <1 CPUS, 2 GB RAM>，然后把这两个 task description 发送给 master。
4. master 把这个两个 task 发送给 slave 1，slave 1 启动 Framework1 的 executor（如果 executor 不存在的话），并把 task 信息发送给 executor，由 executor 负责启动两个 task。

此时由于 slave 1 还剩 <1 CPU, 1 GB> ，所以 master 就可以把它们分配给其他 framework。

![resource offer](/img/mesos/resource-offer.png)

## DRF: Dominant Resource Fairness

前面我们提到 Mesos master 会根据某种分配策略把 resource offer 分配给 framework，Mesos 默认情况下使用的是一种称为 Dominant Resource Fairness（后面简称 DRF） 的分配策略。

### Max-min Fairness

在介绍 DRF 之前，我们先来看单一资源场景下（比如只有 CPU 资源）的分配策略 [max-min fairness](http://www.ece.rutgers.edu/~marsic/Teaching/CCN/minmax-fairsh.html)，之所以要介绍这个策略，是因为 DRF 可以看作 max-min fairness 策略在多资源场景（比如有 <CPUS, MEM> 两种资源）的应用。

Max-min fairness 策略是最大化系统中一个用户收到的最小资源。这里不打算给出形式化的描述，有兴趣的读者可以参考前面给出的链接，我们直接给出 Python 的实现（代码只是为了演示逻辑，边界情况和运行效率没有考虑），然后用一个例子来演示一下。

```python
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
    max_min_fairness(demands, capacity)

# $ python src/mesos/max_min_fair.py
# 0 0.50 0.17 [2, 2.6666666666666665, 2.6666666666666665, 2.6666666666666665]
# 1 0.07 0.03 [2, 2.6, 2.6999999999999997, 2.6999999999999997]
```



例子：假设我们资源总量是 10 个单位，现在有 4 个用户分别需要 2，2.6，4，5 个单位的资源。

分配过程：我们用迭代的方式计算 max-min fairness 的分配情况。
- 首先，我们把资源平均分给 4 个用户，即每个人都有 2.5 个单位；
- 第 1 次迭代，我们发现第一个用户只要 2 个单位的资源，那么我们就可以把剩余的 0.5 个单位资源平均分配给其他三位用户，分完后，他们都是 2.66 个单位的资源；
- 第 2 次迭代发现第二个用户只要 2.6 个单位的资源，所以我们把剩余的 0.066... 个单位资源平均分配给剩下的两个用户，分完后，他们的资源数都是 2.7。
- 第 3 次迭代，发现没有剩余资源，终止循环。
所以最终的分配如下所示：

```python
[('u1', 2), ('u2', 2.6), ('u3', 2.6999999999999997), ('u4', 2.6999999999999997)]
```

### DRF

现在我们来看看多资源环境下的 DRF 算法，首先我们需要了解主导资源（dominant resource）和主导份额（dominant share）的概念。先来看个[例子](https://cloudarchitectmusings.com/2015/04/08/playing-traffic-cop-resource-allocation-in-apache-mesos/)：

假设有一个 slave 提供的资源为 <9 CPUS, 18 GB RAM>, Framework1 的每一个 task 需要的资源为 <1 CPUS, 4 GB RAM>，Framework2 的每一个 task 需要 <3 CPUS, 1 GB RAM>。对于 Framework1 而言，一个 task 需要消耗 CPU 资源占总 CPU 的 1/9，内存占 2/9，因此 Framework1 的主导资源是内存。同样，Framework2 的一个 task 需要的 CPU 占 1/3，内存占 1/18，因此 Framework2 的主导资源是 CPU。DRF 会尝试为每个 Framework 提供等量的主导资源，这个量就是主导份额。

对于这个例子而已，DRF 会为 Framework1 分配 <3 CPUS, 12 GB RAM>，可以创建 3 个 task；为 Framework2 分配 <6 CPUS, 2 GB RAM>，可以创建 2 个 task。此时两个 framework 的主导份额相同（12/18 = 6/9）。

下面我们来看一下 DRF 的伪代码（来自 [DRF 论文](https://people.eecs.berkeley.edu/~alig/papers/drf.pdf)），在每一个步中，DRF 选择 dominant share 最小的用户，如果该用户的 task 所需要的资源被满足，则启动一个 task，并更新用户的 dominant share 的值。

![drf algorithm](/img/mesos/drf-alg.png)

下图演示了系统为 framework 分配资源的过程。

![drf process](/img/mesos/drf-process.png)

#### DRF 算法满足的特性

从上面可以看到 DRF 的算法并不难，难点是它作为一个资源分配算法，需要尽可能满足下面这些特性，由于本篇已经过长了，这里不打算证明这些特性，大家可以参考前面给出的论文，这里只是一个简单的描述。

1. Sharing incentive：对于每一个用户而言，共享集群不会比单独使用 1/n 资源的集群坏。
2. Strategy-proofness：用户谎报资源需求，不会得到好的结果。
3. Envy-freeness：一个用户不会羡慕其他用户分配的资源。
4. Pareto efficiency：系统达到“非损人不能利己”的状态。
5. Single resource fairness：对于单个资源，算法退为 max-min fairness 算法。
6. Bottleneck fairness：如果所有用户的 dominant resource 相同，算法退为 max-min fairness 算法。 
7. Population monotonicity：当一个用户离开系统，并释放掉自己的资源时，其他用户已经分配的资源不应该减少。
8. Resource monotonicity：当系统中加入新的可用资源时，用户已经分配的资源不应该减少。

DRF 算法不满足第 8 条，这个发生在集群中加入新的节点时，这种情况发生的频率较低，所以实际中可以忍受。

## 简单的分布式计算框架

最后我们来看看如何使用 [pymesos](https://github.com/douban/pymesos) 编写一个简单 Mesos Framework，完整的代码在[这里](https://github.com/douban/pymesos/tree/master/examples)。下图是我们编写的 Scheduler、Executor 和 Mesos 交互的流程。

![framework](/img/mesos/framework.png)

这里我不打算贴代码，然后逐行解释了。示例比较简单，大家可以下载试玩，下面我们主要解释下重要的知识点。

### PyMesos

PyMesos 是 Douban 开源的一个纯 Python 实现的 Mesos API，这样我们就不需要 `mesos.native` 了，目前 PyMesos 0.2.0 已经用新的 HTTP API 代替了原来的 Protobuf API。我们的例子中使用 PyMesos 库。

### MinimalScheduler

`MinimalScheduler` 是我们自己实现的 Scheduler，继承自 [pymesos.interface.Scheduler](https://github.com/douban/pymesos/blob/master/pymesos/interface.py#L34)，主要是一些回调函数。我们的示例中只实现了 `resourceOffers` 函数，当我们的 Framework 收到 Mesos master 发来的 resource offer 时会被调用，然后我们根据资源的情况，创建 task，并发送消息（driver.launchTasks）给 Mesos master。

### MesosSchedulerDriver

`MesosSchedulerDriver` 为 scheduler 提供了一个和 Mesos master 交流的接口。它主要做下面两件事情：

1. 管理 scheduler 的生命周期（dirver.stop()、driver.run() 等）。
2. 和 Mesos master 交换(driver.launchTasks 等)

### MinimalExecutor

`MinimalExecutor` 是我们自己实现的 Executor，继承自 [pymesos.interface.Executor](https://github.com/douban/pymesos/blob/master/pymesos/interface.py#L280)，我们的 Executor 很简单，就是接收到 task 任务后，启动一个线程，打印一些字符到 stderr，然后 sleep 30 秒。

### MesosExecutorDriver

`MesosExecutorDriver` 的作用和 `MesosSchedulerDriver` 类似，为 Executor 提供了和 Mesos slave 交流的接口。


## 小结

本文首先介绍了 Mesos 的使用场景，接下来我们熟悉了 Mesos 的设计思路和整体架构，然后介绍了 Mesos 的资源分配算法 DRF: Dominant Resource Fairness，最后我们使用 PyMesos 实现了一个简单的分布式计算框架。

## 练习

1. 在本地安装 Mesos，并运行我们编写的 Minimal 分布式计算框架。
2. Mesos 的 famework 一般不会向 Mesos master 报告它需要的资源，那 Mesos master 怎么知道哪个 framework 是 CPU 主导，哪个是 RAM 主导？怎么知道哪个 framework 需要多少资源，然后以 DRF 算法分配给他们？
