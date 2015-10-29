#!/usr/bin/env python
# coding: utf-8

import re

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


version = ""

with open("autumn.py", "r") as f:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        f.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError("No version information")


setup(name="autumn",
      version=version,
      description="A simple Pythonic MySQL ORM.",
      author="ushuz",
      url="https://github.com/ushuz/autumn",
      py_modules=["autumn"],
      license="MIT License",
)
