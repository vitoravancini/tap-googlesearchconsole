#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="tap-googlesearchconsole",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_googlesearchconsole"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
        "google-api-python-client==1.7.8",
        "google-auth==1.6.2",
        "google-auth-httplib2==0.0.3",
        "google-auth-oauthlib==0.2.0",
        "httplib2==0.12.0",
        "uritemplate==3.0.0",
        "oauth2client==3.0.0"
    ],
    entry_points="""
    [console_scripts]
    tap-googlesearchconsole=tap_googlesearchconsole:main
    """,
    packages=find_packages(),
    package_data={
        "schemas": ["tap_googlesearchconsole/schemas/*.json"]
    },
    include_package_data=True,
)
