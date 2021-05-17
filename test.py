
import requests

url = "https://pro.imdb.com/title/tt4154796"
headers = {
    "cookie": 'session-id=130-1485257-9983924; adblk=adblk_no; ubid-main=132-6940483-2822969; x-main="tyVMItbprLJKedcjM7QfzKPjSbg@RpVAyoKlUMEiGCHL@aTgWr8tDrVtWkHK?rR@"; at-main=Atza|IwEBINodx2soRWAwDCO-jFDtnGJ90L3PDJjp1ujbBZEl8OnqKBeCBn8N4GwB24EzNlI3fclrDDXe9M6FDqsxIeBTdXpjDLWAn6jWM2gRtWVA8FbEYgdY4pN5bmomErGdAoQ0Dm2Y1CZKdGFw590edwXJCv4kFGF0yzPYkkofu_63hJ7VrYW8ZJKTjBlfJIJa_kmRBGXf2a4usvYWQULfWpnWIQTI5Y1TvQmfXu7tANCiPTOmJw; sess-at-main="Lw7ODYlnFdHtso10O3rDcBM2KYNz0rvIqTHhlHniLXc="; uu=eyJpZCI6InV1YjZkNjcyZjc0ZmY0NDI0MThjNWMiLCJwcmVmZXJlbmNlcyI6eyJmaW5kX2luY2x1ZGVfYWR1bHQiOmZhbHNlfSwidWMiOiJ1cjEzMjU5Mjc4MCJ9; _uetsid=501765c0b5ed11eba95f4d411f36a30d; _uetvid=cebd5860ab3211ebbbecb34316e755c5; session-id-time=2082787201l; csm-hit=tb:1902MH6KJV33ZJ223VFR+s-1902MH6KJV33ZJ223VFR|1621135861310&t:1621135861310&adb:adblk_yes; session-token=VK20OFTSAEzGA0zF0C9OzByCm0kEGpAks7RBVKGlNxow1dlO8iKlzj1l7dGuNEV5Rb/0dU/y/72T5cXGg00r0XpFwo9kJdGQQobflvDCwRtzTLmw8BJDvxYImNwQkkYuEBQdm/dkPamqUdwLs56OuIOF3NOvwF5hQ8wKDfpPrbUcL+/KJHl0KO9l2jiWuLvYcOjcezzu5Hn9iICEJsMZmErnc/qnzfsLoiGBaG23j0c=',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
}

print(requests.get(url, headers=headers).text)
