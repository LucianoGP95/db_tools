from setuptools import setup, find_packages

long_description = "A simple ORM made in SQLite to manage small databases."

setup(
    name='db_tools',  
    version='1.0.0', 
    long_description=long_description,
    author='Luciano Galán Plaza',
    author_email='lucianogpingindustrial@gmail.com',
    url='https://github.com/LucianoGP95/db_tools',  
    packages=find_packages(),  
    install_requires=[],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.10',
    ],
)
