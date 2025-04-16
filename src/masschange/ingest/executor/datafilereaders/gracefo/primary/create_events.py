import yaml
from datetime import datetime, timedelta
input = '/Users/ira/Test/D.txt'
out = '/Users/ira/Test/D.yaml'
content = []
with open(input, "r") as file:
    for l in file:
        event = {}
        parts = l.split()
        #print(parts)
        t = datetime(2000, 1, 1, 12) + timedelta(seconds=float(parts[0]))
        formatted_date_time = t.strftime("%Y-%m-%d %H:%M:%S GPS")
        #print(formatted_date_time)
        event['spacecraftevent'] = parts[2]
        event['time'] = formatted_date_time
        event['created'] = ' '.join(parts[7:11])
        event['createdby'] = parts[13]
        data = {}
        data['spacecraft'] = parts[1][-1]
        data['metadata'] = ' '.join(parts[3:7])
        event['data'] = data
        print(event)
        content.append(event)
with open(out, 'w') as f:
    yaml.dump(content, f, sort_keys=False)
