
import asyncio
from canvas_parent_api import Canvas

async def main():
    client = Canvas("https://example.com", "token")
    print(dir(client))

if __name__ == "__main__":
    asyncio.run(main())
