import os
import click

# 模板项目的目录结构
TEMPLATE_STRUCTURE = {
    "project_name": {
        "requirements.txt": "crawlsy-spider",

        "produce.py": """from crawlsy_spider.craw import CrawLsy
from task import demo

with CrawLsy("{{project_name}}") as craw:
    # 创建任务逻辑
    craw.submit(demo)""",

        "consumers.py": """from crawlsy_spider import CrawLsy

with CrawLsy('{{project_name}}') as crawlsy:
    crawlsy.run_work()""",

        "task.py": """
def demo():
    print('task demo function')""",

    }
}


def create_project_structure(project_name):
    """根据模板结构生成项目文件和目录"""
    project_dir = os.path.join(os.getcwd(), project_name)
    if os.path.exists(project_dir):
        raise FileExistsError(f"项目目录 '{project_name}' 已存在")

    os.makedirs(project_dir)

    for path, content in TEMPLATE_STRUCTURE["project_name"].items():
        file_path = os.path.join(project_dir, path.replace("{{project_name}}", project_name))
        file_dir = os.path.dirname(file_path)

        if not os.path.exists(file_dir):
            os.makedirs(file_dir)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content.replace("{{project_name}}", project_name))

    print(f"项目 '{project_name}' 创建完成！")


@click.group()
def cli():
    """命令行工具入口"""
    pass


@click.command()
@click.argument("project_name")
def new(project_name):
    """创建一个新的项目模板"""
    try:
        create_project_structure(project_name)
    except FileExistsError as e:
        click.echo(str(e))


cli.add_command(new)

if __name__ == "__main__":
    cli()
