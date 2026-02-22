from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="cassachange",
    version="1.0.0",
    description="Cassandra schema migration tool — versioned CQL migrations for Cassandra & AstraDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    packages=find_packages(),
    install_requires=[
        "cassandra-driver>=3.25",
        "pyyaml>=6.0",
    ],
    entry_points={
        "console_scripts": [
            "cassachange=cassachange.main:main",
        ],
    },
    classifiers=[
    "Programming Language :: Python :: 3",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    ],
)
