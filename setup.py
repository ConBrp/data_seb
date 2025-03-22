from setuptools import setup

setup(
    name='data_seb',
    version='0.0.1',
    packages=['data_seb'],
    install_requires=[
        "pandas",
        "requests",
        "yfinance",
    ],
    author="Santiago E. Bergese",
    author_email="Santiagobergese@gmail.com",
)