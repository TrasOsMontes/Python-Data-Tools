from distutils.core import setup
setup(
  name = 'GeoLib',
  packages = ['geocodelib'], # this must be the same as the name above
  version = '0.1',
  description = 'A library to retrieve street addresses from Open Street Maps and return a formatted address dictionary that could be easily referenced by your code.',
  author = 'Dom DaFonte',
  author_email = 'me@domdafonte.com',
  url = 'https://github.com/peterldowns/mypackage', # use the URL to the github repo
  download_url = 'https://github.com/peterldowns/mypackage/archive/0.1.tar.gz', # I'll explain this in a second
  keywords = ['geocoding', 'address retrieval', 'Open Street Maps'], # arbitrary keywords
  classifiers = [],
)
