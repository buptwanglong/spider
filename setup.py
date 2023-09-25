from setuptools import setup
import os
import re
import ast

ROOT_PATH = os.path.abspath(os.path.dirname(__file__))

_version_re = re.compile(r"__version__\s+=\s+(.*)")
_init_file = os.path.join(ROOT_PATH, "spider", "__init__.py")
with open(_init_file, "rb") as f:
    version = str(ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1)))


def install_requires():
    with open(os.path.join(ROOT_PATH, 'requirements.txt'), 'r') as f:
        data = f.readlines()
        return data


setup(
    name="spider",
    version=version,
    install_requires=install_requires(),
    author="wanglong",
    author_email="bupt_wanglong@163.com",
    python_requires=">=3.7,",
    entry_points={
        "console_scripts": [
            "spider = spider.__main__:main",
        ]
    }
)
