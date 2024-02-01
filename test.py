# from multiprocessing import Process


# def func1():
#     print("func1: starting")
#     for i in range(10000000):
#         pass

#     print("func1: finishing")


# def func2():
#     print("func2: starting")
#     for i in range(10000000):
#         pass

#     print("func2: finishing")


# if __name__ == "__main__":
#     p1 = Process(target=func1)
#     p1.start()
#     p2 = Process(target=func2)
#     p2.start()
#     p1.join()
#     p2.join()

import asyncio


async def connect_a():
    for i in range(1,100):
        print("a", i)
    await asyncio.sleep(1)
    print("fff")
async def connect_b():
    for i in range(1,100):
        print("b", i)

async def main():
    await asyncio.gather(connect_a(), connect_b())

asyncio.run(main())