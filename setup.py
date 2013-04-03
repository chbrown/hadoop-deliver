from setuptools import setup

setup(
    name='hadoop-deliver',
    version='0.2.0',
    author='Christopher Brown',
    author_email='chrisbrown@utexas.edu',
    packages=['deliver'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'paramiko'
    ],
    entry_points={
        'console_scripts': [
            'hadoop-deliver = deliver:main'
        ],
    },
)
