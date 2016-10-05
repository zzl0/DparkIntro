---
layout: post
title: DPark 漫谈 -- 目录
tags: DPark
comments: yes
og_image_url: ""
description: "Dpark 系列文章的目录."
date: 2014-08-05
---

接下来的一段时间我打算写一个 Dpark 系列的文章, [GitHub 地址](https://github.com/zzl0/DparkIntro)。

主要是两个原因：

1. 梳理一下自己对 Dpark 的理解。
2. 给 Dpark 的新用户一个参考的文档，在这里我不仅会提到各个接口，也会给出一些真实的使用示例，另外还有一些实现方面的细节。

下面是该系列的一个大概的提纲，在写的过程中可能会调整。

第一章 [简介](./01-dpark-basic.md)

1. 什么是 DPark
2. Why DPark
3. 应用示例
4. 小结
5. 练习

第二章 [RDD](./02-dpark-rdd.md)

1. RDD from scratch
2. 词频统计剖析
3. RDD 接口及其应用
4. 小结
5. 练习

第三章 [编写一个基于 Mesos 的分布式框架](./03-mesos.md)

1. 什么是 Mesos
2. Mesos 的设计
3. DRF: Dominant Resource Fairness
4. 简单的分布式计算框架
5. 小结
6. 练习

第四章 整体流程

1. Shuffle
2. Stage 的划分
3. 从 Python 文件到 Mesos task
4. 小结
5. 练习

第五章 性能调优

1. 参数的调优
2. 程序的结构变换
3. 一些 FAQ
4. 小结
5. 练习

第六章 扩展

1. 添加新的文件格式
2. 添加新的函数
3. 小结
4. 练习
