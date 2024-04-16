from setuptools import setup, find_packages

setup(
    name='sparrowSql',
    version='0.1.1',
    description='基于各数据库官方包的二次开发SQL连接工具',
    packages=find_packages(),
    python_requires='>=3.11',
    install_requires=['pymysql', 'requests']
)
