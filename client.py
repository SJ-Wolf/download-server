from urllib.parse import urlencode
import requests

# import aiohttp
# import asyncio
# async def fetch(session, url):
#     async with session.get(url) as response:
#         return await response.text()
#
#
# async def main():
#     async with aiohttp.ClientSession() as session:
#         html = await fetch(session, 'http://python.org')
#         print(html)
#
#
# loop = asyncio.get_event_loop()
# loop.run_until_complete(main())

print(requests.get('http://127.0.0.1:8080/get_url?' + urlencode(dict(url="http://python.org", wait_for_response=True))).text)
print(requests.get('http://127.0.0.1:8080/get_url?' + urlencode(dict(url="https://docs.python.org/3/"))).text)
print(requests.get('http://127.0.0.1:8080/get_url?' + urlencode(dict(url="https://www.python.org/about/legal/"))).text)
print(requests.get('http://127.0.0.1:8080/get_url?' + urlencode(dict(url="https://www.piaotian.com/html/3/3028/1473227.html", wait_for_response=True))).content)
