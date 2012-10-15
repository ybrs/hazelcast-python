from setuptools import setup

setup(
    name='hazelcast',
    version='0.1',
    long_description=__doc__,
    packages=['hazelcast'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[],
    entry_points = {
        'console_scripts': [
            'hazelcast_console = hazelcast.console:run',
        ],
    }
)
