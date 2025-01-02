from tests.test_functions import test  # 导入test函数

from crawlsy_spider.craw import CrawLsy


with CrawLsy("tests", is_async=True) as craw:
    result = craw.submit(test, 'https://baidu.com')
    print(result.results())

