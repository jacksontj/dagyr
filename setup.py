try:
    from setuptools import setup
    from setuptools import find_packages
except ImportError:
    from distutils.core import setup

config = {
    'description': 'DAG execution library',
    'author': 'Thomas Jackson',
    'url': 'https://github.com/jacksontj/dagyr',
    #'download_url': 'Where to download it.',
    'author_email': 'jacksontj.89@gmail.com',
    'version': '0.1',
    'packages': ['dagyr'],
    'scripts': [],
    'name': 'dagyr'
}

setup(**config)
