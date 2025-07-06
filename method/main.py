"""
在这个文件中会存在：
1. 基本的并发执行框架
2. 多agent的连接定义与输入输出定义
3. 测试脚本的执行

整个系统的初始输入为一个query语句与数据库字符串，然后通过InfoAgent与SqlAgent的合作交互完成最后的SQL生成（输出）。

"""

def main():
    query = "What is the daily change in the total market value (formatted as a string in USD currency format) of the USDC token (with a target address of \"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48\") in 2023 , considering both Mint (the input starts with 0x42966c68) and Burn (the input starts with 0x40c10f19) transactions?"
    db_id = "CRYPTO"