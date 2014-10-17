首先，请 clone 我的 git 仓库

```
$ git clone git@github.com:zzl0/DparkIntro.git
```

非豆瓣同学还需要注意两点：

- 自己安装 DPark

```
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

- 示例中的运行方式 `python wc.py -m mesos shakespeare.txt` 中 -m mesos
的意思是以 mesos 集群模式执行。如果你没有配置的话，没法直接这样调用。不过 DPark
有本地模式，可以这样 `python wc.py -m local shakespeare.txt` 执行。
这里的数据不大，本地模式也是很快可以结束的。