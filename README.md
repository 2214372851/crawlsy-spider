# CrawLsy-Spider

## 简介
CrawLsy-Spider 是一个基于 Redis 和 RQ 的爬虫任务管理系统，旨在简化爬虫任务的提交和管理。

## 安装
1. 确保已安装 Python 3.9 或更高版本。
2. 安装依赖库：
```bash
pip install crawlsy-spider
```

## 使用方法

### 初始化项目
```shell
crawlsy-spider new myproject
```

### 在 `task.py` 中编写任务逻辑

```python
import requests

def task_func(url):
    return requests.get(url).text
```

### 在 `produce.py` 提交任务
```python
from crawlsy_spider.craw import CrawLsy

from task import task_func  # 导入test函数

with CrawLsy("tests", is_async=True) as craw:
    result = craw.submit(task_func, 'https://baidu.com')
```

### 工作节点部署
```shell
python worker.py
```

### 运行生产节点

> 由于框架是生产消费分离模式，所以在多服务器（集群中启动 worker），此时服务并不能运行，还需要在新启动一个节点用来启动生产服务

```shell
python producer.py
```

## 开源协议

[GNU General Public License v2.0](LICENSE)

Crawlsy 是自由软件，您可以根据自由软件基金会发布的 GNU 通用公共许可证（版本 2）的条款重新分发或修改它。

发布此程序是希望它有用，但不提供任何保证；甚至没有对适销性或特定用途适用性的暗示保证。有关更多详细信息，请参阅 GNU 通用公共许可证。

您应该已经收到了 GNU 通用公共许可证的副本；如果没有，请参阅 <http://www.gnu.org/licenses/>。