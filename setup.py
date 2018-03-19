"""
Setup Module to setup Python Handlers for the MLW Plugin.
"""
import setuptools

setuptools.setup(
    name='mlstools',
    version='0.1.0',
    packages=setuptools.find_packages(),
    install_requires=[
        'requests'
    ]
)
