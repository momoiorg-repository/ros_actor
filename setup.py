from setuptools import setup, find_packages

setup(
    name='ros_actor',
    version="0.0.1",
    description="python package for actor platform for ROS2",
    author='Kazuya Tago',
    packages=['ros_actor'],
    entry_points={
        'console_scripts': [
            'ros_actor = ros_actor.__main__:main',
        ]
    },
    license='MIT'
)
