from helpers import get_kusto_token, load_state
import requests

state = load_state()
token = get_kusto_token(state['query_service_uri'])
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
url = state['query_service_uri'] + '/v1/rest/query'

for table in ['SensorReading', 'SensorAlert']:
    body = {'db': 'EH_SensorTelemetry', 'csl': f'{table} | count'}
    r = requests.post(url, headers=headers, json=body, timeout=30)
    data = r.json()
    print(f'{table}: {data["Tables"][0]["Rows"][0][0]}')
