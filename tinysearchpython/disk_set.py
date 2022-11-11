from persistqueue import PDict

seen_urls = PDict("seen_urls", "seen_urls_name", multithreading=True)
print("some_url" in seen_urls)
seen_urls["some_url"] = "P"
print("some_url" in seen_urls)

