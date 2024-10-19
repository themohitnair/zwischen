# zwischen
Metrics and Analytics middleware for FastAPI endpoints.

## Prerequisites
1. GeoIP.conf
The root of your server must contain a GeoIP.conf file of the following format:

```yaml
AccountID <Account ID>
LicenseKey <your MaxMind GeoIP license key>
EditionIDs GeoLite2-City
DatabaseDirectory </path/to/your/project/root/directory/from/root>
```

Good news: You can get to know more about this file (barring the last line "DatabaseDirectory" from the MaxMind website at the link: [GeoIP Database Updates](https://dev.maxmind.com/geoip/updating-databases/))

2. Environment Variables
The root of your server must contain a `.env` file of the following format:

```C
MAXMIND_GEOIP_LICENSE=<your MaxMind GeoIP license key>
SERVER_PATH=</path/to/your/project/root/directory/from/root>
```