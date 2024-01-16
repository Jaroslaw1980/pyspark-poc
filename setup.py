from setuptools import setup, find_packages

setup(
    name='PySparkApp',
    version='1.0',
    author='Jarosław Nazar',
    author_email='jaroslaw.nazar@capgemini.com',
    url='https://github.com/Jaroslaw1980/pyspark-poc.git',
    description='PySpark poc with loading, filtering, dropping joining and saving 2 csv files',
    license='MIT licence',
    packages=find_packages(exclude=["test"]),
    install_requires='requirement.txt',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ]
)
