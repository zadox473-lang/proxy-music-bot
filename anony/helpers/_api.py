import asyncio
import aiohttp
import aiofiles

from anony import logger

class API:
    def __init__(
            self, api_url: str, api_key: str,
            retries: int = 10, timeout: int = 10,
        ):
        self.api_url = api_url
        self.api_key = api_key
        self.chunk_limit = 1024 * 1024
        self.dl_cache = {}
        self.v_cache = {}
        self.retries = retries
        self.dl_endp = self.api_url + "/youtube/v2/download"
        self.job_endp = self.api_url + "/youtube/jobStatus"
        self.params = {"api_key": self.api_key}
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session: aiohttp.ClientSession | None = None


    async def get_session(self) -> None:
        self.session = aiohttp.ClientSession(timeout=self.timeout)

    async def close(self) -> None:
        if self.session:
            await self.session.close()


    async def create_job(self, video_id: str, video: bool = False) -> str | None:
        _params = self.params.copy()
        _params["query"] = video_id
        _params["isVideo"] = str(video).lower()

        for _ in range(3):
            try:
                async with self.session.get(self.dl_endp, params=_params) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(1)
                        continue

                    _data = await resp.json()
                    if _data.get("status") != "queued":
                        await asyncio.sleep(1)
                        continue
                        
                    job_id = _data.get("job_id")
                    if not job_id:
                        await asyncio.sleep(1)
                        continue
                        
                    return job_id
            except Exception:
                await asyncio.sleep(1)
                
        return None


    async def get_url(self, job_id: str) -> str | None:
        _params = {"job_id": job_id}
        for attempt in range(1, self.retries + 1):
            try:
                async with self.session.get(self.job_endp, params=_params) as resp:
                    if resp.status != 200: 
                        await asyncio.sleep(3)
                        continue

                    _data = await resp.json()
                    status = _data.get("status")

                    if status != "success":
                        await asyncio.sleep(3)
                        continue

                    job = _data.get("job", {})
                    if job.get("status", "") != "done":
                        await asyncio.sleep(3)
                        continue

                    _url = job.get("result", {}).get("public_url")
                    if not _url:
                        break
                        
                    logger.info(f"ArcApi: Received #{attempt} [{self.api_url + _url}]")
                    return self.api_url + _url
            except Exception:
                pass
                
            await asyncio.sleep(3)
        return None


    async def save_file(self, url: str) -> str | None:
        fpath = "downloads/" + url.split("/")[-1]
        try:
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    return None

                async with aiofiles.open(fpath, "wb") as f:
                    async for chunk in resp.content.iter_chunked(self.chunk_limit):
                        if chunk: await f.write(chunk)

                return fpath
        except Exception:
            pass
        return None


    async def download(self, vid_id: str, video: bool = False) -> str | None:
        if video and vid_id in self.v_cache:
            return self.v_cache[vid_id]
        elif not video and vid_id in self.dl_cache:
            return self.dl_cache[vid_id]

        for attempt in range(2):
            job_id = await self.create_job(vid_id, video)
            if not job_id:
                if attempt == 0: await asyncio.sleep(2)
                continue

            dl_url = await self.get_url(job_id)
            if not dl_url:
                if attempt == 0: await asyncio.sleep(2)
                continue

            fpath = await self.save_file(dl_url)
            if not fpath:
                if attempt == 0: await asyncio.sleep(2)
                continue

            if video:
                self.v_cache[vid_id] = fpath
            else:
                self.dl_cache[vid_id] = fpath
                
            return fpath

        return None
