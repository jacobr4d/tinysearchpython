class Url:
    """
    scheme    {http or https}
    host      {google}
    path      {parsed}
    port      {parsed or guessed based on scheme}

    Throws error if malformed
    Anchors (#) and parameters (?) removed
    """
    def __init__(self, url):
        assert(url.startswith("http") or url.startswith("https"))
        if url.startswith("https"):
            self.scheme = "https"
            url = url[8:]
        else:
            self.scheme = "http"
            url = url[7:]
        if "?" in url:
            url = url[:url.index("?")]
        if "#" in url:
            url = url[:url.index("#")]
        if "/" in url:
            self.path = url[url.index("/"):]
            url = url[:url.index("/")]
        else:
            self.path = "/"
        if ":" in url:
            self.host = url[:url.index(":")]
            self.port = url[url.index(":") + 1:]
        else:
            self.host = url
            self.port = 443 if self.scheme == "https" else 80 
    
    def __copy__(self):
        ret = Url("https://doesnt:80/matter")
        ret.scheme = self.scheme
        ret.host = self.host
        ret.path = self.path
        ret.port = self.port
        return ret

    def __str__(self):
        return f"{self.scheme}://{self.host}:{self.port}{self.path}"