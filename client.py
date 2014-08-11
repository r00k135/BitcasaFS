# Test Client for Bitcasa Python SDK
from bitcasa import Bitcasa

# Start Client
client = Bitcasa('config.json')
print("Bitcasa has been authorized with your account.")
print("### Folder List (/) ###")
folder_info = client.list_folder('/')
folder_path = folder_info[0]['path']
print(folder_info)
print(folder_path)
print("### Adding Folder (to root)###")
print(client.add_folder(folder_path, "Test"))
