from setuptools import setup

setup(
    name='XTB-Trades',
    author='Jan Kwiatkowski',
    author_email='kwiatkor9@gmail.com',
    description='Trading project based on the xStation5 API Python library',
    long_description=open('README.md', encoding='utf-8').read(),
    license='MIT',
    keywords='python python3 bitcoin trading websocket trading-api forex xapi forex-trading exchange-api forex-data xstation xstation5 xtb xopenhub forex-api xopenhub-api xtb-api xstation-api x-trade-brokers bfbcapital',
    url='https://github.com/jkwiatk1/XTB_Trading',
    classifiers=[
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12"
    ],
    python_requires='>=3.9',
    version="0.1.6.1",
    packages=['xapi'],
)