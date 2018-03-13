# cn_stock_tick
- cn_stock_tick项目可以获取从1991年至程序运行当天A股所有股票的分笔数据并将其存入mongodb中，支持断点续传和增量。

# prerequisite
项目运行在python3.5上，依赖于
- pytdx: https://github.com/rainx/pytdx
- tdx: https://github.com/JaysonAlbert/tdx
如果以后还有另外的需求的话，再考虑另行增加requirement.txt和pip install。

# install & run
```bash
git clone https://github.com/jockerxlj/cn_stock_tick.git . 
```
修改```config/tick.conf```文件, 将db_host和db_port改为指定的内网mongodb服务器
```
db_host=
db_port=
```
运行main.py
```
python main.py
```
如果运行过程中程序异常终止或者没有一段时间没有任何响应的话可以直接kill掉再运行```python main.py```即可，写过的数据不会重复写。


