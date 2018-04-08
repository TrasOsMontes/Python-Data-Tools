from geopy.geocoders import Nominatim #Used for grabbing geocode info from open street maps
from geopy.exc import GeocoderTimedOut
import time
import datetime


def do_geocode(street, city, state, unit_number=''):
	street = street.title().strip()
	city = city.title().strip()
	state = state.upper().strip()
	address = '"' + street + ' ' + city + ', ' + state + '"'
	try:
		addresses = dict()
		time.sleep(0.5 )   # delays for 1 seconds. You can Also Use Float Value.
		geolocator = Nominatim(scheme='http', user_agent='user-Bot-parcelminer')
		geo = geolocator.geocode(address, addressdetails=True, timeout=50)
		print(datetime.datetime.now(), ': ', geo)
		addresses['street_number'] = street.split(' ')[0].split(',')[0] if (street != '' and street.isdigit()) else None
		addresses['street'] = street
		addresses['city'] = city
		addresses['state'] = state
		addresses['unit_number'] = unit_number
		if geo is not None:
			for key, value in geo.raw.items():
				if key == 'place_id':
					addresses['place_id'] = value
				elif key == 'osm_id':
					addresses['osm_id'] = value
				elif key == 'address':
					for subKey, subValue in value.items():
						if subKey == 'country_code':
							addresses['country_code'] = subValue
						else:
							addresses['country_code'] = ''
						if subKey == 'county':
							addresses['county'] = subValue
						else:
							addresses['county'] = ''
						if subKey == 'postcode':
							addresses['postcode'] = subValue
						else:
							addresses['postcode'] = ''
			
			addresses['longitude'] = geo.longitude
			addresses['latitude'] = geo.latitude
		else:
			addresses.update({'place_id' : '',
								'osm_id' : '',
								'country_code' : '',
								'county' : '',
								'postcode' : '',
								'latitude' : '',
								'longitude' : ''})
		if addresses['unit_number'] != '':
			street = street + ', ' + addresses['unit_number']
		if addresses['postcode'] != '':
			addresses['address'] = street +' '+ city +', '+ state + ' '+ addresses['postcode']
		else:
			addresses['address'] = street +' '+ city +', ' + state
		addresses['slug'] = addresses['address'].replace(' ', '-').replace(',', '')

		return addresses

	except GeocoderTimedOut:
		return do_geocode(street, city, state)

