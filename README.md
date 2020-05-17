
# Fitbit API exporter

The project is a simple script to export data from Fitbit's Web APIs to a custom InfluxDB database, and then graph it via Grafana.  
Everything is made easier via docker and docker-compose.

## Quick setup
Notes:
- basic knowledge of docker is assumed
- unless otherwise noted, leave fields at default
- the script sources environment variables if missing, otherwise sources the saved config data (this is because the token gets refreshed automatically thanks to the refresh token)
- if you don't run the scripts on a server, you may need to refresh the access token and refresh token at the next run, because the refresh token may have already expired (see below)

Step-by-step guide:
1. Do a test-run of the docker-compose.yml provided, and customize it to your liking, ensure the containers come up, esp. the InfluxDB one
1. Go at [Fitbit dev login](https://dev.fitbit.com/login), login with your account
1. Register an application: click on "Register an app" at the top
    - Application name and description to your liking
    - Application website, Organization website, terms of service, privacy policy, callback URL, you can all set "http://localhost:8080/"
    - OAuth 2.0 Application Type: Personal
    - Default Access Type: Read-Only
1. Get first two parameters: click on "Manage my apps", click on your new application; you will need to note down the following in order to fill corresponding env vars:
    - OAuth 2.0 Client ID: CLIENT_ID
    - Client Secret: CLIENT_SECRET
    - Callback URL: CALLBACK_URL
1. Generate the tokens: click on the small link at the bottom of the page "OAuth 2.0 tutorial page"
    1. Select "Flow type": "Authorization Code Flow"
    1. Select Scopes: activity, heartrate, profile, settings, sleep, weight
    1. Click on the link at the end of section 1 "We've generate the authorization URL for you, all you need to do is just click on the link below:"
    1. Select all scopes and click "Allow"
    1. Copy the code parameter from the URL of the window that opens: code=[.....]#_=_
    1. Paste it in the "1A Get Code" form
    1. Copy the curl call to a script, remove newlines, execute the script
    1. Copy the JSON output from the script and paste into the "2: Parse response" section
    1. Note down:
        - Access token: ACCESS_TOKEN
        - Refresh Token: REFRESH_TOKEN
1. You can fill the provided `docker-compose.yml` with the parameters obtained
1. Pull up the containers

## Limitations

### Fitbit APIs
1. Fitbit APIs are limited to 150calls/hour, the script will detect this and sleep for 1h 10s
1. The script will sleep for 4h when there's no additional data to fetch, and only fetch the last day at each round

### Intra-day time series
**Intra-day time series are not implemented**.  
I originally wanted to add them, but saw little benefit after finishing the normal time-series.  
One additional issue with intra-day series is that they require additional setup on Fitbit's side, and you may need to request access to those via Fitbit support. I have not tried contacting them because I had no need for it.  
One last note is that intra-day data is much more granular (1m/30s/1s intervals), so each query will return a lot more data to download and store, which I had no need for.

## Development
`api_poller.py` is all there is to it, use `docker-compose-dev.yml` to pull up a local instance for testing

### Deps
Python script, developed with:
- [Fitbit API client](https://github.com/orcasgit/python-fitbit.git)
- [InfluxDB client](https://github.com/influxdata/influxdb-python.git)


