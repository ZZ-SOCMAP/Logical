import asyncio
import logging

import grpc

from proto import logical_pb2, logical_pb2_grpc


class Service(logical_pb2_grpc.LogicalHandlerServicer):

    async def ping(self, request, context) -> logical_pb2.reply:
        # 健康检测接口, 禁止删除
        return logical_pb2.reply(status=True, message="pong")

    async def call(self, request, context) -> logical_pb2.reply:
        # 实际处理逻辑实现(此处模拟测试)
        logging.info(f" Got a message: [TABLE: {request.table}, ID: {request.id}, OPERATE: {request.operate}]")
        import random
        number = random.randint(1, 10)
        if number > 8:
            return logical_pb2.reply(status=False, message=f"failure: {number}")
        return logical_pb2.reply(status=True, message="success")


async def serve() -> None:
    server = grpc.aio.server()
    logical_pb2_grpc.add_LogicalHandlerServicer_to_server(Service(), server)

    listen_addr = "[::]:50049"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        await server.stop(0)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())
