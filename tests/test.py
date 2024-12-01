import requests, json

def pull_user_data1():
    user_data = requests.get('http://localhost:8000/users')
    return json.dumps(user_data.json())

def pull_user_data2():
    user_data = requests.get('http://localhost:8000/users')
    return user_data.json()

print(pull_user_data1())
print(pull_user_data2())

