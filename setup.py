from setuptools import setup, find_packages

long_description = 'A longer description of your package'

setup(
    name='db_tools',  # Replace with your package name
    version='1.0.0',  # Replace with your package version
    description='ORM for SQLite',
    long_description=long_description,
    author='Luciano Galán Plaza',
    author_email='your@email.com',
    url='https://github.com/yourusername/yourrepository',  # Replace with your GitHub repository URL
    packages=find_packages(),  # Automatically find and include all packages
    install_requires=[],  # List your package's dependencies here
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.10',
    ],
)
