from setuptools import find_packages, setup

long_description = open("README.md").read()


setup(
    name='machy',
    packages=find_packages(include=['machy']),
    version='1.0.1',
    description='Web scraper utils',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/Ennoriel/machy',
    author='Maxime Dupont',
    license='MIT',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    test_suite='tests',
)