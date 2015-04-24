import base64

# Start your middleware class
class ProxyMiddleware(object):
  # overwrite process request
  def process_request(self, request, spider):
    # Set the location of the proxy
    request.meta['proxy'] = "http://104.131.150.165:8080"