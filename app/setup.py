# app/setup.py

from setuptools import find_packages, setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="kafka_sqlmodel",
    version="0.1.2",
    description="A KAFKA client to produce and consume events using SQLModel",
    package_dir={"": "app"},
    packages=find_packages(where="app"),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    author="NB",
    author_email="nimish.bhatia@alucor.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "confluent-kafka>=2.6.0",
        "sqlmodel>=0.0.22",
    ],
    extras_require={
        "dev": ["pytest>=8.3.3", "twine>=5.1.1", "setuptools-75.3.0"],
    },
    python_requires=">=3.12",
)