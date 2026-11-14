"""Equivalent bounded file operations for the optional guest I/O comparison."""
import os, hashlib

class BenchIO:
    chunk_size = 65536
    def __init__(self, kind):
        if kind not in ('sync', 'threads', 'uringio'):
            raise ValueError(kind)
        self.kind = kind
        self.runner = None
        self.asyncio = None
        self.uringio = None
        if kind != 'sync':
            import asyncio
            self.asyncio = asyncio
            if kind == 'uringio':
                import uringio
                self.uringio = uringio
            else:
                from concurrent.futures import ThreadPoolExecutor
                self.pool_type = ThreadPoolExecutor
        self.fd_before = len(os.listdir('/proc/self/fd'))
        if kind != 'sync':
            factory = self.uringio.UringioLoop if self.uringio else self.asyncio.new_event_loop
            self.runner = self.asyncio.Runner(loop_factory=factory)
            loop = self.runner.get_loop()
            if kind == 'threads':
                loop.set_default_executor(self.pool_type(max_workers=4))

    @staticmethod
    def _hash_sync(path):
        digest = hashlib.sha256()
        with open(path, 'rb', buffering=0) as file:
            remaining = os.stat(path).st_size
            while remaining:
                data = file.read(min(BenchIO.chunk_size, remaining))
                if not data:
                    raise OSError('input truncated during read')
                digest.update(data)
                remaining -= len(data)
        return digest.hexdigest()

    async def _hash_uring(self, path):
        digest = hashlib.sha256()
        offset = 0
        # openat2 requires mode=0 without O_CREAT/O_TMPFILE. uringio defaults to 0644.
        async with await self.uringio.open_file(path=str(path), flags=os.O_RDONLY, mode=0) as file:
            size = os.stat(path).st_size
            while offset < size:
                data = await file.read(offset=offset, size=min(self.chunk_size, size-offset))
                if not data:
                    raise OSError('input truncated during read')
                digest.update(data)
                offset += len(data)
        return digest.hexdigest()

    async def _hash_all(self, paths):
        limit = self.asyncio.Semaphore(4)
        async def one(path):
            async with limit:
                if self.kind == 'threads':
                    return await self.asyncio.to_thread(self._hash_sync, path)
                return await self._hash_uring(path)
        tasks = [self.asyncio.create_task(one(path)) for path in paths]
        try:
            return await self.asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await self.asyncio.gather(*tasks, return_exceptions=True)
            raise

    def hash_files(self, paths):
        values = ([self._hash_sync(path) for path in paths] if self.kind == 'sync'
                  else self.runner.run(self._hash_all(paths)))
        return {path.name: value for path, value in zip(paths, values)}

    @staticmethod
    def _write_sync(path, data, count):
        with open(path, 'wb', buffering=0) as file:
            for _ in range(count):
                offset = 0
                while offset < len(data):
                    written = file.write(data[offset:])
                    if written <= 0:
                        raise OSError('short write without progress')
                    offset += written

    async def _write_uring(self, path, data, count):
        offset = 0
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        async with await self.uringio.open_file(path=str(path), flags=flags, mode=0o644) as file:
            for _ in range(count):
                part = 0
                while part < len(data):
                    written = await file.write(data=data[part:], offset=offset)
                    if written <= 0:
                        raise OSError('short write without progress')
                    part += written
                    offset += written

    async def _write_thread(self, path, data, count):
        await self.asyncio.to_thread(self._write_sync, path, data, count)

    def write(self, path, data, count=1):
        if self.kind == 'sync':
            self._write_sync(path, data, count)
        elif self.kind == 'threads':
            self.runner.run(self._write_thread(path, data, count))
        else:
            self.runner.run(self._write_uring(path, data, count))

    def close(self):
        if self.runner:
            self.runner.close()
        delta = len(os.listdir('/proc/self/fd')) - self.fd_before
        # Report retained descriptors even for experimental backends with broken
        # cleanup. Each benchmark request ends by destroying its guest process.
        return delta
