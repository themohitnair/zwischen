import asyncio
import aiohttp
import random
from faker import Faker

fake = Faker()

ENDPOINTS = ['/', '/something']

METHODS = ['GET', 'POST', 'PUT']

USER_AGENTS = [
    # Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/91.0.864.41',
    
    # macOS
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15',
    
    # Linux
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; CrOS x86_64 13601.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    
    # iOS
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1',
    
    # Android
    'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 10; Pixel 3 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36',
    
    # Other User Agents
    'Mozilla/5.0 (Linux; Android 8.1.0; Nexus 5X Build/OPM1.171019.012) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 9; SM-J530F Build/PPR1.180610.011) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 5.1; Nexus 6 Build/LMY48M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 11; Moto G Stylus 5G Build/RKQ1.201210.002) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36',
    'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
]


async def send_request(session, url):
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'X-Forwarded-For': fake.ipv4(),
        'Referer': fake.uri()
    }
    method = random.choice(METHODS)
    endpoint = random.choice(ENDPOINTS)
    
    try:
        async with session.request(method, f"{url}{endpoint}", headers=headers) as response:
            await response.text()
            print(f"Sent {method} request to {endpoint} with status {response.status}")
    except Exception as e:
        print(f"Error sending request: {e}")

async def main():
    url = "http://localhost:8000"
    num_requests = 10000

    async with aiohttp.ClientSession() as session:
        tasks = [send_request(session, url) for _ in range(num_requests)]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())